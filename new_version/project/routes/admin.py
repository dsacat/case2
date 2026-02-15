import csv
import io
import json as _json
import re
from datetime import timedelta

from flask import Blueprint, redirect, render_template, request, flash, make_response
from main import (
    app, db, build_base_context, require_user, require_roles, message_page,
    Users, Dish, DishGroup, DishReview, MealOrder, Session, WeeklyMenu,
    can_change_user_role, allowed_roles_to_assign, create_notification,
    get_cfg, cfg_bool, set_cfg, DEFAULT_CFG, contact_data_to_raw,
    save_project_settings_from_request, refresh_runtime_config,
    role_label, USER_ROLES,
    to_int, to_float, func, datetime,
    DISH_ICON_DIR, save_as_avif, Image,
    get_console_allowed_commands, get_console_command_specs,
    is_role, build_console, role_level, get_csrf_token, is_valid_csrf_request,
)
import main as _main_module

admin_bp = Blueprint('admin', __name__)


@admin_bp.route('/create_menu_group/', methods=['GET', 'POST'])
def create_menu_group():
    user, failure = require_roles({'admin', 'super_admin'})
    if failure:
        return failure

    mes = ''
    form_data = {'title': '', 'description': '', 'sort_order': ''}
    field_errors = {}

    if request.method == 'POST':
        for key in form_data.keys():
            form_data[key] = request.form.get(key, '').strip()

        title = re.sub(r'\s+', ' ', form_data['title']).strip()
        description = re.sub(r'\s+', ' ', form_data['description']).strip()
        sort_order = None
        if form_data['sort_order']:
            sort_order = to_int(form_data['sort_order'], -1)

        if len(title) < 2:
            mes = 'Название группы меню слишком короткое.'
            field_errors['title'] = True
        elif len(title) > 120:
            mes = 'Название группы меню слишком длинное.'
            field_errors['title'] = True
        elif len(description) > 260:
            mes = 'Описание группы меню слишком длинное.'
            field_errors['description'] = True
        elif sort_order is not None and sort_order < 0:
            mes = 'Порядок сортировки должен быть неотрицательным числом.'
            field_errors['sort_order'] = True
        else:
            group = DishGroup.query.filter(func.lower(DishGroup.title) == title.lower()).first()
            if not group:
                if sort_order is None:
                    max_order = db.session.query(func.max(DishGroup.sort_order)).scalar() or 0
                    sort_order = max_order + 10
                group = DishGroup(
                    title=title,
                    description=description,
                    sort_order=sort_order,
                    is_active=True,
                    created_by=user.id,
                    updated_at=datetime.utcnow(),
                )
                db.session.add(group)
                db.session.commit()
                flash('Группа меню успешно создана.', 'success')
                return redirect(f'/create_dish/?group_id={group.id}')

            group.description = description
            if sort_order is not None:
                group.sort_order = sort_order
            if not group.is_active:
                group.is_active = True
            group.updated_at = datetime.utcnow()
            db.session.commit()
            flash('Группа меню обновлена.', 'success')
            return redirect(f'/create_dish/?group_id={group.id}')

    return render_template('create_menu_group.html', **build_base_context(
        user,
        mes=mes,
        form_data=form_data,
        field_errors=field_errors,
    ))


@admin_bp.route('/create_dish/', methods=['GET', 'POST'])
def create_dish():
    user, failure = require_roles({'admin', 'super_admin'})
    if failure:
        return failure

    mes = ''
    form_data = {k: '' for k in ['dish_title', 'dish_description', 'dish_composition', 'allergens', 'dish_mass',
                                 'dish_kcal', 'dish_proteins', 'dish_fats', 'dish_carbs', 'dish_price',
                                 'dish_group_new']}
    form_data['dish_category'] = 'lunch'
    form_data['dish_group_id'] = '0'
    field_errors = {}

    if request.method == 'GET':
        preselected_group_id = to_int(request.args.get('group_id', '0'), 0)
        if preselected_group_id > 0:
            preselected_group = DishGroup.query.filter_by(id=preselected_group_id, is_active=True).first()
            if preselected_group:
                form_data['dish_group_id'] = str(preselected_group.id)

    if request.method == 'POST':
        for key in form_data.keys():
            form_data[key] = request.form.get(key, '').strip()

        title = form_data['dish_title']
        description = form_data['dish_description']
        composition = form_data['dish_composition']
        allergens = form_data['allergens']
        category = form_data['dish_category'] if form_data['dish_category'] in {'breakfast', 'lunch'} else 'lunch'
        mass = to_float(form_data['dish_mass'], -1)
        kcal = to_float(form_data['dish_kcal'], -1)
        proteins = to_float(form_data['dish_proteins'], -1)
        fats = to_float(form_data['dish_fats'], -1)
        carbs = to_float(form_data['dish_carbs'], -1)
        price = to_int(form_data['dish_price'], -1)
        selected_group_id = to_int(form_data['dish_group_id'], 0)
        new_group_title = re.sub(r'\s+', ' ', form_data['dish_group_new']).strip()

        if not title or not description or not composition:
            mes = 'Заполните название, описание и состав блюда.'
            if not title:
                field_errors['dish_title'] = True
            if not description:
                field_errors['dish_description'] = True
            if not composition:
                field_errors['dish_composition'] = True
        elif len(title) > 220:
            mes = 'Название блюда слишком длинное (максимум 220 символов).'
            field_errors['dish_title'] = True
        elif len(description) > 2000:
            mes = 'Описание блюда слишком длинное (максимум 2000 символов).'
            field_errors['dish_description'] = True
        elif len(composition) > 2000:
            mes = 'Состав блюда слишком длинный (максимум 2000 символов).'
            field_errors['dish_composition'] = True
        elif len(allergens) > 500:
            mes = 'Список аллергенов слишком длинный (максимум 500 символов).'
            field_errors['allergens'] = True
        elif min(mass, kcal, proteins, fats, carbs) < 0 or price < 0:
            mes = 'Проверьте числовые поля: значения не могут быть отрицательными.'
            if mass < 0:
                field_errors['dish_mass'] = True
            if kcal < 0:
                field_errors['dish_kcal'] = True
            if proteins < 0:
                field_errors['dish_proteins'] = True
            if fats < 0:
                field_errors['dish_fats'] = True
            if carbs < 0:
                field_errors['dish_carbs'] = True
            if price < 0:
                field_errors['dish_price'] = True
        elif selected_group_id < 0:
            mes = 'Некорректная группа меню.'
            field_errors['dish_group_id'] = True
        else:
            selected_group = None
            if new_group_title:
                if len(new_group_title) < 2:
                    mes = 'Название группы меню слишком короткое.'
                    field_errors['dish_group_new'] = True
                elif len(new_group_title) > 120:
                    mes = 'Название группы меню слишком длинное.'
                    field_errors['dish_group_new'] = True
                else:
                    selected_group = DishGroup.query.filter(
                        func.lower(DishGroup.title) == new_group_title.lower()).first()
                    if not selected_group:
                        max_order = db.session.query(func.max(DishGroup.sort_order)).scalar() or 0
                        selected_group = DishGroup(
                            title=new_group_title,
                            description='',
                            sort_order=max_order + 10,
                            is_active=True,
                            created_by=user.id,
                            updated_at=datetime.utcnow(),
                        )
                        db.session.add(selected_group)
                        db.session.flush()
                    elif not selected_group.is_active:
                        selected_group.is_active = True
                        selected_group.updated_at = datetime.utcnow()
                    form_data['dish_group_id'] = str(selected_group.id)
                    form_data['dish_group_new'] = ''
            elif selected_group_id > 0:
                selected_group = DishGroup.query.filter_by(id=selected_group_id, is_active=True).first()
                if not selected_group:
                    mes = 'Выбранная группа меню не найдена.'

            dish_group_id = None
            if selected_group:
                dish_group_id = selected_group.id
            elif selected_group_id > 0:
                dish_group_id = selected_group_id

            if not mes:
                dish = Dish(
                    title=title,
                    description=description,
                    composition=composition,
                    allergens=allergens,
                    category=category,
                    mass_grams=mass,
                    calories=kcal,
                    proteins=proteins,
                    fats=fats,
                    carbohydrates=carbs,
                    price=price,
                    dish_group_id=dish_group_id,
                    is_active=True,
                    created_by=user.id,
                    updated_at=datetime.utcnow(),
                )
                db.session.add(dish)
                db.session.commit()

                image = request.files.get('dish_image')
                if image and image.filename:
                    try:
                        img = Image.open(image).convert('RGBA')
                        w, h = img.size
                        side = min(w, h)
                        img = img.crop(((w - side) / 2, (h - side) / 2, (w + side) / 2, (h + side) / 2))
                        img = img.resize((900, 900), Image.LANCZOS)
                        output_name = f'{dish.id}.avif'
                        save_as_avif(img, DISH_ICON_DIR / output_name)
                        dish.image_path = f'icons/dishes/{output_name}'
                        db.session.commit()
                    except IOError as e:
                        app.logger.warning(f'Invalid image file for dish {dish.id}: {str(e)}')
                        flash('Блюдо добавлено, но изображение не загружено (неверный формат).', 'warning')
                    except Exception as e:
                        app.logger.error(f'Failed to process dish image for dish {dish.id}: {str(e)}')
                        flash('Блюдо добавлено, но ошибка при обработке изображения. Попробуйте позже.', 'warning')

                flash('Блюдо успешно добавлено в меню.', 'success')
                return redirect(f'/dish/{dish.id}/')

    dish_groups = (
        DishGroup.query
        .filter(DishGroup.is_active == True)
        .order_by(DishGroup.sort_order.asc(), DishGroup.title.asc())
        .all()
    )

    return render_template('create_dish.html', **build_base_context(
        user,
        mes=mes,
        form_data=form_data,
        field_errors=field_errors,
        dish_groups=dish_groups,
        categories={'breakfast': 'Завтрак', 'lunch': 'Обед'},
    ))


@admin_bp.route('/settings/', methods=['GET', 'POST'])
def settings():
    user, failure = require_user(role_level('admin'))
    if failure:
        return failure

    if request.method == 'POST':
        if not is_valid_csrf_request():
            flash('Недействительный CSRF-токен.', 'error')
            return redirect('/settings/')
        action = request.form.get('action', 'save_settings').strip()
        if action == 'set_role':
            target_user_id = to_int(request.form.get('target_user_id', '0'), 0)
            new_role = request.form.get('new_role', '').strip()
            target_user = db.session.get(Users, target_user_id) if target_user_id > 0 else None
            if not target_user:
                flash('Пользователь не найден.', 'error')
            elif new_role not in USER_ROLES:
                flash('Некорректная роль.', 'error')
            elif not can_change_user_role(user, target_user, new_role):
                flash('Недостаточно прав для изменения этой роли.', 'error')
            else:
                old_role = target_user.role
                target_user.role = new_role
                db.session.commit()
                create_notification(
                    target_user.id,
                    'Роль изменена',
                    f'Новая роль: {role_label(new_role)}',
                    '/profile/',
                )
                db.session.commit()
                flash(
                    f'Роль пользователя {target_user.email} изменена: {role_label(old_role)} -> {role_label(new_role)}.',
                    'success')
            return redirect('/settings/')

        asset_errors = save_project_settings_from_request(request.form, request.files)
        if asset_errors:
            flash(' '.join(asset_errors), 'error')
        else:
            flash('Настройки сохранены.', 'success')
        return redirect('/settings/')

    settings_data = {
        'site_name': get_cfg('Name', DEFAULT_CFG['Name']),
        'school_name': get_cfg('Name_sch', DEFAULT_CFG['Name_sch']),
        'contacts_raw': contact_data_to_raw(get_cfg('contact_data', DEFAULT_CFG['contact_data'])),
        'host': get_cfg('adress', DEFAULT_CFG['adress']),
        'port': get_cfg('port', DEFAULT_CFG['port']),
        'protection': get_cfg('protection', DEFAULT_CFG['protection']),
        'debug': cfg_bool('debug', DEFAULT_CFG['debug']),
        'bg_blur': 0,
        'ico_path': get_cfg('ico_path', DEFAULT_CFG['ico_path']),
        'bg_path': get_cfg('bg_path', DEFAULT_CFG['bg_path']),
        'mail_enabled': cfg_bool('mail_enabled', DEFAULT_CFG['mail_enabled']),
        'mail_server': get_cfg('mail_server', DEFAULT_CFG['mail_server']),
        'mail_port': get_cfg('mail_port', DEFAULT_CFG['mail_port']),
        'mail_use_tls': cfg_bool('mail_use_tls', DEFAULT_CFG['mail_use_tls']),
        'mail_username': get_cfg('mail_username', DEFAULT_CFG['mail_username']),
        'mail_password_mask': ('*' * 8) if get_cfg('mail_password', DEFAULT_CFG['mail_password']) else '',
        'announcement': get_cfg('announcement', ''),
        'announcement_type': get_cfg('announcement_type', 'info'),
        'low_balance_threshold': to_int(get_cfg('low_balance_threshold', DEFAULT_CFG['low_balance_threshold']), DEFAULT_CFG['low_balance_threshold']),
        'topup_max_amount': to_int(get_cfg('topup_max_amount', DEFAULT_CFG['topup_max_amount']), DEFAULT_CFG['topup_max_amount']),
    }
    assignable_roles = sorted(list(allowed_roles_to_assign(user)), key=lambda role: USER_ROLES[role]['level'])
    role_users = Users.query.filter(Users.is_active == True).order_by(Users.role.asc(), Users.surname.asc(),
                                                                      Users.name.asc()).limit(400).all()
    return render_template(
        'settings.html',
        **build_base_context(
            user,
            settings_data=settings_data,
            form_data={},
            field_errors={},
            assignable_roles=assignable_roles,
            role_users=role_users,
            can_change_user_role=can_change_user_role,
        ),
    )


@admin_bp.route('/admin_console/', methods=['GET', 'POST'])
def admin_console():
    user, failure = require_roles({'super_admin'})
    if failure:
        return failure

    is_super = is_role(user, 'super_admin')
    allowed_commands = get_console_allowed_commands(user)
    command_specs = get_console_command_specs(user)
    if not allowed_commands:
        return message_page('Недостаточно прав доступа.', user=user)

    if is_super:
        if _main_module.admin_console_runner is None:
            _main_module.admin_console_runner = build_console(mode=True, log_file='console.txt')
            _main_module.admin_console_runner.start_console()
        runner = _main_module.admin_console_runner
        console_mode_label = 'Полный доступ'
    else:
        if _main_module.admin_console_runner_admin is None:
            _main_module.admin_console_runner_admin = build_console(mode=True, log_file='console_admin.txt')
            _main_module.admin_console_runner_admin.start_console()
        runner = _main_module.admin_console_runner_admin
        console_mode_label = 'Ограниченный доступ'

    if request.method == 'POST':
        if not is_valid_csrf_request():
            flash('Недействительный CSRF-токен.', 'error')
            return redirect('/admin_console/')
        command = request.form.get('command', '').strip()
        if command:
            runner.execute_command(command, echo=True, allowed_commands=allowed_commands)
        return redirect('/admin_console/')

    command_templates = [item for item in runner.command_templates if item.get('command') in allowed_commands]
    if not command_templates:
        command_templates = [{'title': item['title'], 'command': cmd} for cmd, item in command_specs.items()]

    lines = runner.get_log().splitlines()
    return render_template(
        'admin.html',
        **build_base_context(
            user,
            log=lines,
            command_templates=command_templates,
            command_specs=command_specs,
            console_mode_label=console_mode_label,
        ),
    )


@admin_bp.route('/admin/dashboard/')
def admin_dashboard():
    user, failure = require_roles({'admin', 'super_admin'})
    if failure:
        return failure

    now = datetime.utcnow()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=today_start.weekday())
    month_start = today_start.replace(day=1)
    paid_statuses = ('ordered', 'issued', 'received')

    def revenue_since(dt):
        val = db.session.query(func.sum(MealOrder.price)).filter(
            MealOrder.status.in_(paid_statuses),
            MealOrder.created_at >= dt
        ).scalar()
        return val or 0

    revenue_today = revenue_since(today_start)
    revenue_week = revenue_since(week_start)
    revenue_month = revenue_since(month_start)

    top_dishes_rows = (
        db.session.query(Dish.title, func.count(MealOrder.id).label('cnt'))
        .join(MealOrder, MealOrder.dish_id == Dish.id)
        .filter(MealOrder.status.in_(paid_statuses))
        .group_by(Dish.id, Dish.title)
        .order_by(func.count(MealOrder.id).desc())
        .limit(10)
        .all()
    )

    thirty_days_ago = now - timedelta(days=30)
    active_users_count = (
        db.session.query(Session.user_id)
        .filter(Session.is_active == True, Session.last_seen >= thirty_days_ago)
        .distinct()
        .count()
    )

    charts_json = _json.dumps({
        'top_dishes': {
            'labels': [r.title for r in top_dishes_rows],
            'data': [r.cnt for r in top_dishes_rows],
        }
    })

    return render_template('dashboard.html', **build_base_context(
        user,
        revenue_today=revenue_today,
        revenue_week=revenue_week,
        revenue_month=revenue_month,
        active_users_count=active_users_count,
        charts_json=charts_json,
    ))


@admin_bp.route('/admin/dashboard/export.csv')
def admin_dashboard_export_csv():
    user, failure = require_roles({'admin', 'super_admin'})
    if failure:
        return failure

    paid_statuses = ('ordered', 'issued', 'received')
    rows = (
        db.session.query(
            Dish.title.label('dish_name'),
            func.count(MealOrder.id).label('order_count'),
            func.sum(MealOrder.price).label('revenue'),
            func.avg(MealOrder.price).label('avg_price'),
        )
        .join(MealOrder, MealOrder.dish_id == Dish.id)
        .filter(MealOrder.status.in_(paid_statuses))
        .group_by(Dish.id, Dish.title)
        .order_by(func.count(MealOrder.id).desc())
        .all()
    )

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(['dish_name', 'order_count', 'revenue', 'avg_price'])
    for r in rows:
        writer.writerow([r.dish_name, r.order_count, r.revenue or 0, round(r.avg_price or 0, 2)])

    response = make_response(buf.getvalue())
    response.headers['Content-Type'] = 'text/csv; charset=utf-8'
    response.headers['Content-Disposition'] = 'attachment; filename="dashboard_stats.csv"'
    return response


@admin_bp.route('/menu/schedule/', methods=['GET', 'POST'])
def menu_schedule():
    user, failure = require_roles({'admin', 'super_admin'})
    if failure:
        return failure

    DAY_NAMES = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']

    if request.method == 'POST':
        if not is_valid_csrf_request():
            flash('Недействительный CSRF-токен.', 'error')
            return redirect('/menu/schedule/')
        action = request.form.get('action', '')
        if action == 'add':
            day = to_int(request.form.get('day_of_week', ''), -1)
            dish_id = to_int(request.form.get('dish_id', ''), 0)
            if 0 <= day <= 6 and dish_id:
                dish = Dish.query.filter_by(id=dish_id, is_active=True).first()
                if dish:
                    existing = WeeklyMenu.query.filter_by(dish_id=dish_id, day_of_week=day).first()
                    if not existing:
                        db.session.add(WeeklyMenu(dish_id=dish_id, day_of_week=day, created_by=user.id))
                        db.session.commit()
                        flash('Блюдо добавлено в расписание.', 'success')
                    else:
                        flash('Это блюдо уже назначено на выбранный день.', 'error')
                else:
                    flash('Блюдо не найдено.', 'error')
            else:
                flash('Некорректные данные формы.', 'error')
        elif action == 'remove':
            entry_id = to_int(request.form.get('entry_id', ''), 0)
            entry = db.session.get(WeeklyMenu, entry_id)
            if entry:
                db.session.delete(entry)
                db.session.commit()
                flash('Блюдо удалено из расписания.', 'success')
            else:
                flash('Запись не найдена.', 'error')
        return redirect('/menu/schedule/')

    schedule_rows = WeeklyMenu.query.order_by(WeeklyMenu.day_of_week, WeeklyMenu.id).all()
    by_day = {i: [] for i in range(7)}
    for row in schedule_rows:
        by_day[row.day_of_week].append(row)

    active_dishes = Dish.query.filter_by(is_active=True).order_by(Dish.title).all()

    return render_template('menu_schedule.html', **build_base_context(
        user,
        by_day=by_day,
        day_names=DAY_NAMES,
        active_dishes=active_dishes,
        csrf_token=get_csrf_token(),
    ))


admin = admin_bp
