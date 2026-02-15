from flask import Blueprint, g, jsonify, redirect, render_template, request, flash
from main import (
    db, build_base_context, require_user, require_roles, message_page,
    Users, Dish, DishGroup, DishReview, MealOrder, WeeklyMenu,
    build_menu_groups, dish_image_path, get_allergen_warnings,
    get_parent_children_rows, build_child_display_name, is_parent_of_student,
    get_student_restrictions, check_dish_against_restrictions, check_dish_against_limits, get_student_daily_spent,
    parse_meal_date, create_notification, PaymentOperation,
    has_permission, role_level, is_role,
    is_valid_csrf_request,
    to_int, to_float, func, datetime, date
)

menu = Blueprint('menu', __name__)

ALLERGEN_KEYWORDS = {
    'глютен': ['глютен', 'пшениц', 'мук', 'хлеб', 'макарон', 'крупа', 'манн', 'ячмен', 'рожь', 'овёс', 'овес'],
    'лактоза': ['лактоз', 'молок', 'сливк', 'масл', 'сыр', 'творог', 'кефир', 'йогурт', 'сметан', 'молочн'],
    'орехи': ['орех', 'миндал', 'фундук', 'кешью', 'грецк', 'арахис', 'фисташ'],
    'яйца': ['яйц', 'яйко', 'омлет', 'желток', 'белок яйц'],
    'рыба': ['рыб', 'лосос', 'треск', 'сельд', 'тунец', 'скумбри', 'минтай', 'судак', 'карп'],
    'соя': ['соя', 'сои', 'соев'],
    'кунжут': ['кунжут', 'тахин', 'сезам'],
}


def detect_dish_allergens(dish):
    explicit = [a.strip().lower() for a in (dish.allergens or '').split(',') if a.strip()]
    if explicit:
        return explicit
    text = ' '.join([dish.title or '', dish.composition or '', dish.description or '']).lower()
    return [key for key, words in ALLERGEN_KEYWORDS.items() if any(w in text for w in words)]


@menu.route('/')
def index():
    user = g.current_user
    today_dow = date.today().weekday()
    DAY_NAMES_RU = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    current_day_name = DAY_NAMES_RU[today_dow]
    schedule_entries = WeeklyMenu.query.filter_by(day_of_week=today_dow).all()
    if schedule_entries:
        scheduled_ids = {entry.dish_id for entry in schedule_entries}
        dishes = Dish.query.filter(Dish.is_active == True, Dish.id.in_(scheduled_ids)).order_by(Dish.created_at.desc()).all()
        has_schedule = True
    else:
        dishes = Dish.query.filter_by(is_active=True).order_by(Dish.created_at.desc()).all()
        has_schedule = False
    menu_groups = build_menu_groups(dishes)

    orders_preview = []
    if user:
        target_ids = [user.id]
        if user.role == 'parent':
            target_ids = [child.id for _, child in get_parent_children_rows(user.id, active_only=True)]
            if not target_ids:
                target_ids = [-1]
        records = (
            db.session.query(MealOrder, Dish, Users)
            .join(Dish, Dish.id == MealOrder.dish_id)
            .join(Users, Users.id == MealOrder.user_id)
            .filter(MealOrder.user_id.in_(target_ids))
            .order_by(MealOrder.created_at.desc())
            .limit(6)
            .all()
        )
        for order, dish, order_user in records:
            from main import parse_order_status_label
            entry = {'id': order.id, 'dish': dish.title, 'dish_id': dish.id, 'status': parse_order_status_label(order.status),
                     'price': order.price, 'created': order.created_at.strftime('%d.%m %H:%M')}
            if user.role == 'parent':
                entry['child'] = build_child_display_name(order_user)
            orders_preview.append(entry)

    top_dishes_rows = (
        db.session.query(Dish, func.avg(DishReview.rating).label('avg_r'), func.count(DishReview.id).label('cnt'))
        .join(DishReview, DishReview.dish_id == Dish.id)
        .filter(Dish.is_active == True)
        .group_by(Dish.id)
        .having(func.count(DishReview.id) >= 1)
        .order_by(func.avg(DishReview.rating).desc())
        .limit(5)
        .all()
    )
    top_dishes = [{'dish': d, 'avg_rating': round(r or 0, 1)} for d, r, _ in top_dishes_rows]

    dish_allergens = {dish.id: detect_dish_allergens(dish) for dish in dishes}
    saved_allergens = []
    if user:
        saved_allergens = (user.dop_data or {}).get('allergens', [])
    return render_template('index.html', **build_base_context(
        user,
        menu_groups=menu_groups,
        orders_preview=orders_preview,
        top_dishes=top_dishes,
        dish_allergens=dish_allergens,
        saved_allergens=saved_allergens,
        can_create_dish=bool(user and has_permission(user, role_level('admin'))),
        can_open_kitchen=bool(user and is_role(user, 'chef')),
        can_open_reports=bool(user),
        current_day_name=current_day_name,
        has_schedule=has_schedule,
    ))


@menu.route('/menu/group/<group_key>/')
def menu_group_page(group_key):
    user = g.current_user
    dishes = Dish.query.filter_by(is_active=True).order_by(Dish.created_at.desc()).all()
    menu_groups = build_menu_groups(dishes)
    group = next((item for item in menu_groups if item['key'] == group_key), None)
    if not group:
        return message_page('Группа меню не найдена.', user=user)

    group_ids = [dish.id for dish in group['dishes']]
    rate_rows = []
    if group_ids:
        rate_rows = (
            db.session.query(DishReview.dish_id, func.avg(DishReview.rating), func.count(DishReview.id))
            .filter(DishReview.dish_id.in_(group_ids))
            .group_by(DishReview.dish_id)
            .all()
        )
    ratings = {row[0]: {'avg': round(row[1] or 0, 1), 'count': row[2]} for row in rate_rows}

    group_db_id = None
    if group.get('kind') == 'custom':
        key_parts = group['key'].split('_', 1)
        if len(key_parts) == 2 and key_parts[1].isdigit():
            group_db_id = int(key_parts[1])
    return render_template('menu_group.html', **build_base_context(
        user,
        menu_group=group,
        ratings=ratings,
        dish_image_path=dish_image_path,
        can_create_dish=bool(user and has_permission(user, role_level('admin'))),
        group_db_id=group_db_id,
    ))


@menu.route('/dish/<int:dish_id>/')
def dish_information(dish_id):
    user = g.current_user
    dish = Dish.query.get_or_404(dish_id)
    if not dish.is_active and not (user and has_permission(user, role_level('admin'))):
        return message_page('Блюдо недоступно.', user=user)

    reviews_raw = (
        db.session.query(DishReview, Users)
        .join(Users, Users.id == DishReview.user_id)
        .filter(DishReview.dish_id == dish.id)
        .order_by(DishReview.created_at.desc())
        .all()
    )
    reviews = []
    user_review = None
    for review, author in reviews_raw:
        item = {
            'id': review.id,
            'author': f'{author.surname} {author.name}'.strip(),
            'rating': review.rating,
            'text': review.review_text,
            'date': review.created_at.strftime('%d.%m.%Y %H:%M'),
        }
        reviews.append(item)
        if user and review.user_id == user.id:
            user_review = item

    parent_children = []
    if user and user.role == 'parent':
        for link, child in get_parent_children_rows(user.id, active_only=True):
            parent_children.append({
                'id': child.id,
                'name': build_child_display_name(child),
                'daily_limit': to_int(link.daily_limit, 0),
            })

    can_order = bool(user and user.role in {'student', 'parent'})
    order_block_reason = ''
    order_low_balance = False
    payer_balance = to_int(user.balance, 0) if user else 0

    if not user:
        order_block_reason = 'Чтобы заказать блюдо, войдите в аккаунт школьника или родителя.'
    elif user.role not in {'student', 'parent'}:
        order_block_reason = 'Заказ доступен только школьнику или родителю для привязанного ребенка.'
    elif user.role == 'parent' and not parent_children:
        order_block_reason = 'Сначала привяжите ребенка в профиле, чтобы оформить заказ.'
    elif payer_balance < to_int(dish.price, 0):
        order_low_balance = True

    rate_row = db.session.query(func.avg(DishReview.rating), func.count(DishReview.id)).filter(
        DishReview.dish_id == dish.id).first()
    allergen_warnings = get_allergen_warnings(user, dish)
    return render_template('dish_information.html', **build_base_context(
        user,
        dish=dish,
        reviews=reviews,
        user_review=user_review,
        avg_rating=round(rate_row[0] or 0, 1),
        rate_count=rate_row[1] or 0,
        can_order=can_order,
        dish_image=dish_image_path(dish),
        parent_children=parent_children,
        order_block_reason=order_block_reason,
        order_low_balance=order_low_balance,
        payer_balance=payer_balance,
        allergen_warnings=allergen_warnings,
        is_favorite=dish.id in [int(x) for x in ((user.dop_data or {}).get('favorites') or []) if str(x).isdigit()] if user else False,
        can_edit_dish=bool(user and has_permission(user, role_level('admin'))),
    ))


@menu.route('/dish/<int:dish_id>/favorite/', methods=['POST'])
def toggle_favorite(dish_id):
    user, failure = require_user(1)
    if failure:
        return jsonify({'status': 'error', 'message': 'Требуется авторизация'}), 401
    dish = Dish.query.get_or_404(dish_id)
    dop = dict(user.dop_data or {})
    favs = [int(x) for x in (dop.get('favorites') or []) if str(x).isdigit()]
    if dish.id in favs:
        favs.remove(dish.id)
        added = False
    else:
        favs.append(dish.id)
        added = True
    dop['favorites'] = favs
    user.dop_data = dop
    db.session.commit()
    return jsonify({'status': 'ok', 'added': added, 'count': len(favs)})


@menu.route('/dish/<int:dish_id>/review/', methods=['POST'])
def dish_review(dish_id):
    user, failure = require_user(1)
    if failure:
        return failure
    if not is_valid_csrf_request():
        return message_page('Недействительный CSRF-токен.', user=user)
    dish = Dish.query.get_or_404(dish_id)
    rating = max(1, min(5, to_int(request.form.get('rating', 5), 5)))
    text_body = request.form.get('review_text', '').strip()[:2000]

    existing = DishReview.query.filter_by(dish_id=dish.id, user_id=user.id).first()
    if existing:
        existing.rating = rating
        existing.review_text = text_body
        existing.updated_at = datetime.utcnow()
    else:
        db.session.add(DishReview(dish_id=dish.id, user_id=user.id, rating=rating, review_text=text_body))
    db.session.commit()
    flash('Отзыв сохранен.', 'success')
    return redirect(f'/dish/{dish.id}/')


@menu.route('/dish/<int:dish_id>/order/', methods=['POST'])
def order_dish(dish_id):
    if not g.get('current_user'):
        flash('Для оформления заказа выполните вход.', 'error')
        return redirect('/login/new/')
    user, failure = require_roles({'student', 'parent'})
    if failure:
        return failure
    if not is_valid_csrf_request():
        return message_page('Недействительный CSRF-токен.', user=user)

    dish = Dish.query.get_or_404(dish_id)
    if not dish.is_active:
        return message_page('Блюдо недоступно для заказа.', user=user)

    target_date = parse_meal_date(request.form.get('meal_date', ''))
    if target_date < date.today():
        flash('Нельзя заказать на прошедшую дату.', 'error')
        return redirect(f'/dish/{dish.id}/')

    target_user = user
    payer_user = user
    if user.role == 'parent':
        child_id = to_int(request.form.get('child_id', '0'), 0)
        if child_id <= 0:
            flash('Выберите ребенка для заказа.', 'error')
            return redirect(f'/dish/{dish.id}/')
        target_user = Users.query.filter_by(id=child_id, role='student', is_active=True).first()
        if not target_user or not is_parent_of_student(user.id, target_user.id):
            flash('Ребенок не привязан к вашему аккаунту.', 'error')
            return redirect(f'/dish/{dish.id}/')

    restrictions = get_student_restrictions(target_user.id)
    product_error = check_dish_against_restrictions(dish, restrictions)
    if product_error:
        flash(product_error, 'error')
        return redirect(f'/dish/{dish.id}/')
    limits_error = check_dish_against_limits(dish, restrictions)
    if limits_error:
        flash(limits_error, 'error')
        return redirect(f'/dish/{dish.id}/')

    daily_limit = to_int(restrictions.get('daily_limit', 0), 0)
    if daily_limit > 0:
        spent_today = get_student_daily_spent(target_user.id, target_date)
        limit_label = 'ребенка' if user.role == 'parent' else ''
        limit_text = f'Превышен дневной лимит {limit_label}: {daily_limit} ₽ (уже заказано {spent_today} ₽).'.replace(
            '  ', ' ')
        if spent_today + dish.price > daily_limit:
            flash(limit_text, 'error')
            return redirect(f'/dish/{dish.id}/')

    if payer_user.balance < dish.price:
        need_more = dish.price - payer_user.balance
        flash(f'Недостаточно средств на балансе. Нужно пополнить минимум на {need_more} ₽.', 'error')
        return redirect('/pay/')

    payer_user.balance -= dish.price
    db.session.add(
        MealOrder(
            user_id=target_user.id,
            payer_user_id=payer_user.id,
            dish_id=dish.id,
            price=dish.price,
            status='ordered',
            meal_date=target_date,
        )
    )
    db.session.add(
        PaymentOperation(
            user_id=payer_user.id,
            target_user_id=target_user.id,
            amount=-dish.price,
            kind='dish_order',
            description=f'Заказ блюда: {dish.title}',
        )
    )
    create_notification(
        payer_user.id,
        'Заказ оформлен',
        f"{dish.title} на {target_date.strftime('%d.%m.%Y')}",
        '/profile/',
    )
    if target_user.id != payer_user.id:
        create_notification(
            target_user.id,
            'Родитель оформил заказ',
            f"{dish.title} на {target_date.strftime('%d.%m.%Y')}",
            '/profile/',
        )
    db.session.commit()

    if target_user.id == payer_user.id:
        flash('Заказ успешно оформлен.', 'success')
    else:
        flash(f'Заказ оформлен для: {build_child_display_name(target_user)}.', 'success')
    return redirect('/profile/')


@menu.route('/dish/<int:dish_id>/edit/', methods=['GET'])
def edit_dish_get(dish_id):
    user, failure = require_roles({'admin', 'super_admin'})
    if failure:
        return failure
    dish = Dish.query.get_or_404(dish_id)
    dish_groups = DishGroup.query.filter_by(is_active=True).order_by(DishGroup.sort_order).all()
    categories = {'breakfast': 'Завтрак', 'lunch': 'Обед'}
    return render_template('edit_dish.html', **build_base_context(user, dish=dish, dish_groups=dish_groups, categories=categories))


@menu.route('/dish/<int:dish_id>/edit/', methods=['POST'])
def edit_dish_post(dish_id):
    user, failure = require_roles({'admin', 'super_admin'})
    if failure:
        return failure
    dish = Dish.query.get_or_404(dish_id)
    dish_title = request.form.get('dish_title', '').strip()
    if not dish_title:
        flash('Название не может быть пустым.', 'error')
        return redirect(f'/dish/{dish.id}/edit/')
    if len(dish_title) > 220:
        flash('Название блюда слишком длинное (максимум 220 символов).', 'error')
        return redirect(f'/dish/{dish.id}/edit/')
    dish_description = request.form.get('dish_description', '').strip()
    if not dish_description:
        flash('Описание не может быть пустым.', 'error')
        return redirect(f'/dish/{dish.id}/edit/')
    if len(dish_description) > 2000:
        flash('Описание блюда слишком длинное (максимум 2000 символов).', 'error')
        return redirect(f'/dish/{dish.id}/edit/')
    dish_composition = request.form.get('dish_composition', '').strip()
    if not dish_composition:
        flash('Состав не может быть пустым.', 'error')
        return redirect(f'/dish/{dish.id}/edit/')
    if len(dish_composition) > 2000:
        flash('Состав блюда слишком длинный (максимум 2000 символов).', 'error')
        return redirect(f'/dish/{dish.id}/edit/')
    dish_allergens = request.form.get('allergens', '').strip()
    if len(dish_allergens) > 500:
        flash('Список аллергенов слишком длинный (максимум 500 символов).', 'error')
        return redirect(f'/dish/{dish.id}/edit/')
    dish_category = request.form.get('dish_category', '')
    if dish_category not in {'breakfast', 'lunch'}:
        dish_category = dish.category
    dish_group_id = to_int(request.form.get('dish_group_id', '0'), 0)
    dish_mass = to_float(request.form.get('dish_mass', '0'), 0)
    dish_kcal = to_float(request.form.get('dish_kcal', '0'), 0)
    dish_proteins = to_float(request.form.get('dish_proteins', '0'), 0)
    dish_fats = to_float(request.form.get('dish_fats', '0'), 0)
    dish_carbs = to_float(request.form.get('dish_carbs', '0'), 0)
    dish_price = to_int(request.form.get('dish_price', '0'), 0)
    if min(dish_mass, dish_kcal, dish_proteins, dish_fats, dish_carbs) < 0 or dish_price < 0:
        flash('Числовые значения блюда не могут быть отрицательными.', 'error')
        return redirect(f'/dish/{dish.id}/edit/')
    dish.title = dish_title
    dish.description = dish_description
    dish.composition = dish_composition
    dish.allergens = dish_allergens
    dish.category = dish_category
    dish.dish_group_id = dish_group_id if dish_group_id > 0 else None
    dish.mass_grams = dish_mass
    dish.calories = dish_kcal
    dish.proteins = dish_proteins
    dish.fats = dish_fats
    dish.carbohydrates = dish_carbs
    dish.price = dish_price
    dish.updated_at = datetime.utcnow()
    db.session.commit()
    flash('Блюдо обновлено.', 'success')
    return redirect(f'/dish/{dish.id}/')


@menu.route('/dish/<int:dish_id>/delete/', methods=['POST'])
def dish_soft_delete(dish_id):
    user, failure = require_roles({'admin', 'super_admin'})
    if failure:
        return failure
    dish = Dish.query.get_or_404(dish_id)
    dish.is_active = False
    dish.updated_at = datetime.utcnow()
    db.session.commit()
    flash('Блюдо скрыто из меню.', 'success')
    return redirect(f'/dish/{dish.id}/')


@menu.route('/dish/<int:dish_id>/restore/', methods=['POST'])
def dish_restore(dish_id):
    user, failure = require_roles({'admin', 'super_admin'})
    if failure:
        return failure
    dish = Dish.query.get_or_404(dish_id)
    dish.is_active = True
    dish.updated_at = datetime.utcnow()
    db.session.commit()
    flash('Блюдо снова активно.', 'success')
    return redirect(f'/dish/{dish.id}/')


@menu.route('/menu-group/<int:group_id>/edit/', methods=['POST'])
def edit_menu_group(group_id):
    user, failure = require_roles({'admin', 'super_admin'})
    if failure:
        return failure
    group = DishGroup.query.get_or_404(group_id)
    title = request.form.get('title', '').strip()
    if not title:
        flash('Название группы не может быть пустым.', 'error')
        return redirect(f'/menu/group/group_{group_id}/')
    if len(title) > 120:
        flash('Название слишком длинное (максимум 120 символов).', 'error')
        return redirect(f'/menu/group/group_{group_id}/')
    existing = DishGroup.query.filter(
        func.lower(DishGroup.title) == title.lower(),
        DishGroup.id != group_id
    ).first()
    if existing:
        flash('Группа с таким названием уже существует.', 'error')
        return redirect(f'/menu/group/group_{group_id}/')
    group.title = title
    group.updated_at = datetime.utcnow()
    db.session.commit()
    flash('Название группы обновлено.', 'success')
    return redirect(f'/menu/group/group_{group_id}/')


@menu.route('/menu-group/<int:group_id>/delete/', methods=['POST'])
def delete_menu_group(group_id):
    user, failure = require_roles({'admin', 'super_admin'})
    if failure:
        return failure
    group = DishGroup.query.get_or_404(group_id)
    active_dishes = Dish.query.filter_by(dish_group_id=group_id, is_active=True).count()
    if active_dishes > 0:
        flash(f'Нельзя удалить группу: в ней {active_dishes} активных блюд.', 'error')
        return redirect(f'/menu/group/group_{group_id}/')
    group.is_active = False
    group.updated_at = datetime.utcnow()
    db.session.commit()
    flash('Группа меню удалена.', 'success')
    return redirect('/')


@menu.route('/menu-group/<int:group_id>/move/', methods=['POST'])
def move_menu_group(group_id):
    user, failure = require_roles({'admin', 'super_admin'})
    if failure:
        return failure
    direction = request.form.get('direction', '')
    if direction not in ('up', 'down'):
        flash('Неверное направление.', 'error')
        return redirect('/')
    group = DishGroup.query.get_or_404(group_id)
    all_groups = (
        DishGroup.query
        .filter_by(is_active=True)
        .order_by(DishGroup.sort_order.asc(), DishGroup.id.asc())
        .all()
    )
    ids = [g.id for g in all_groups]
    if group.id not in ids:
        flash('Группа не найдена.', 'error')
        return redirect('/')
    idx = ids.index(group.id)
    if direction == 'up' and idx > 0:
        neighbor = all_groups[idx - 1]
        group.sort_order, neighbor.sort_order = neighbor.sort_order, group.sort_order
        if group.sort_order == neighbor.sort_order:
            neighbor.sort_order += 1
        group.updated_at = datetime.utcnow()
        neighbor.updated_at = datetime.utcnow()
        db.session.commit()
        flash('Группа перемещена вверх.', 'success')
    elif direction == 'down' and idx < len(all_groups) - 1:
        neighbor = all_groups[idx + 1]
        group.sort_order, neighbor.sort_order = neighbor.sort_order, group.sort_order
        if group.sort_order == neighbor.sort_order:
            group.sort_order += 1
        group.updated_at = datetime.utcnow()
        neighbor.updated_at = datetime.utcnow()
        db.session.commit()
        flash('Группа перемещена вниз.', 'success')
    return redirect('/')


@menu.route('/menu/week/')
def menu_week():
    user = g.current_user
    DAY_NAMES_RU = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    today_dow = date.today().weekday()
    all_entries = (
        WeeklyMenu.query
        .join(Dish, Dish.id == WeeklyMenu.dish_id)
        .filter(Dish.is_active == True)
        .order_by(WeeklyMenu.day_of_week, WeeklyMenu.id)
        .all()
    )
    by_day = {i: [] for i in range(7)}
    for entry in all_entries:
        by_day[entry.day_of_week].append(entry)
    return render_template('menu_week.html', **build_base_context(
        user,
        by_day=by_day,
        day_names=DAY_NAMES_RU,
        today_dow=today_dow,
    ))
