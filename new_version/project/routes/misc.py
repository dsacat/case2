from flask import Blueprint, redirect, render_template, request, flash
import json
from main import (
    db, build_base_context, require_user, require_roles, message_page,
    Users, Notification, FeedbackThread, FeedbackMessage, ParentStudentLink, Dish,
    get_notification_preferences, resolve_notification_category,
    get_parent_children_rows, build_child_display_name,
    enforce_csrf_protection,
    has_permission, role_level, create_notification,
    to_int, datetime
)

misc_bp = Blueprint('misc', __name__)


@misc_bp.route('/feedback/', methods=['GET', 'POST'])
def feedback():
    user, failure = require_user(1)
    if failure:
        return failure

    form_data = {'subject': '', 'body': ''}
    field_errors = {}
    if request.method == 'POST':
        form_data['subject'] = request.form.get('subject', '').strip()
        form_data['body'] = request.form.get('body', '').strip()
        if len(form_data['subject']) > 200:
            flash('Тема слишком длинная (максимум 200 символов).', 'error')
            field_errors['subject'] = True
        elif len(form_data['body']) > 5000:
            flash('Сообщение слишком длинное (максимум 5000 символов).', 'error')
            field_errors['body'] = True
        elif form_data['subject'] and form_data['body']:
            thread = FeedbackThread(user_id=user.id, subject=form_data['subject'], status='open', updated_at=datetime.utcnow())
            db.session.add(thread)
            db.session.flush()
            db.session.add(FeedbackMessage(thread_id=thread.id, user_id=user.id, role=user.role, body=form_data['body']))
            db.session.commit()
            flash('Обращение отправлено.', 'success')
            return redirect(f'/feedback/{thread.id}/')
        flash('Заполните тему и текст обращения.', 'error')
        if not form_data['subject']:
            field_errors['subject'] = True
        if not form_data['body']:
            field_errors['body'] = True

    is_moder = has_permission(user, role_level('moder'))
    if is_moder:
        threads = FeedbackThread.query.order_by(FeedbackThread.updated_at.desc()).limit(120).all()
    else:
        threads = FeedbackThread.query.filter_by(user_id=user.id).order_by(FeedbackThread.updated_at.desc()).limit(
            120).all()
    return render_template('feedback.html', **build_base_context(user, threads=threads, is_moder=is_moder, form_data=form_data, field_errors=field_errors))


@misc_bp.route('/feedback/<int:thread_id>/', methods=['GET', 'POST'])
def feedback_thread(thread_id):
    user, failure = require_user(1)
    if failure:
        return failure
    thread = FeedbackThread.query.get_or_404(thread_id)
    is_moder = has_permission(user, role_level('moder'))
    if thread.user_id != user.id and not is_moder:
        return message_page('Нет доступа к обращению.', user=user)

    if request.method == 'POST':
        body = request.form.get('body', '').strip()
        new_status = request.form.get('status', '').strip()
        if len(body) > 5000:
            flash('Сообщение слишком длинное (максимум 5000 символов).', 'error')
            return redirect(f'/feedback/{thread.id}/')
        if body:
            db.session.add(FeedbackMessage(thread_id=thread.id, user_id=user.id, role=user.role, body=body))
            thread.updated_at = datetime.utcnow()
            if not is_moder and thread.status == 'closed':
                thread.status = 'open'
            if is_moder and thread.user_id != user.id:
                create_notification(thread.user_id, 'Ответ на обращение', thread.subject, f'/feedback/{thread.id}/')
        if is_moder and new_status in {'open', 'closed'}:
            thread.status = new_status
            thread.updated_at = datetime.utcnow()
        if db.session.dirty or db.session.new:
            db.session.commit()
        return redirect(f'/feedback/{thread.id}/')

    messages = (
        db.session.query(FeedbackMessage, Users)
        .join(Users, Users.id == FeedbackMessage.user_id)
        .filter(FeedbackMessage.thread_id == thread.id)
        .order_by(FeedbackMessage.created_at.asc())
        .all()
    )
    return render_template('feedback_thread.html',
                           **build_base_context(user, thread=thread, messages=messages, is_moder=is_moder))


@misc_bp.route('/notifications/')
def notifications():
    user, failure = require_user(1)
    if failure:
        return failure
    if request.args.get('mark') == 'all':
        Notification.query.filter_by(user_id=user.id, is_read=False).update({'is_read': True})
        db.session.commit()
        return redirect('/notifications/')
    state = request.args.get('state', 'all').strip().lower()
    category = request.args.get('category', 'all').strip().lower()
    allowed_states = {'all', 'unread', 'read'}
    allowed_categories = {'all', 'orders', 'payments', 'feedback', 'kitchen', 'system'}
    if state not in allowed_states:
        state = 'all'
    if category not in allowed_categories:
        category = 'all'

    rows = Notification.query.filter_by(user_id=user.id).order_by(Notification.created_at.desc()).limit(400).all()
    prefs = get_notification_preferences(user)
    filtered = []
    for row in rows:
        row_category = resolve_notification_category(row.title, row.link)
        if not prefs.get(row_category, True):
            continue
        if state == 'unread' and row.is_read:
            continue
        if state == 'read' and not row.is_read:
            continue
        if category != 'all' and row_category != category:
            continue
        filtered.append(row)
    return render_template(
        'notifications.html',
        **build_base_context(
            user,
            notifications=filtered,
            filter_state=state,
            filter_category=category,
            notification_categories={
                'all': 'Все',
                'orders': 'Заказы',
                'payments': 'Оплата',
                'feedback': 'Обратная связь',
                'kitchen': 'Кухня',
                'system': 'Системные',
            },
        ),
    )


@misc_bp.route('/parent/limits/', methods=['GET', 'POST'])
def parent_limits():
    user, failure = require_roles({'parent'})
    if failure:
        return failure

    KNOWN_ALLERGENS = [
        'глютен', 'молоко', 'яйца', 'орехи', 'арахис', 'соя',
        'рыба', 'морепродукты', 'сельдерей', 'горчица', 'кунжут',
        'сульфиты', 'люпин', 'моллюски',
    ]

    children_rows = get_parent_children_rows(user.id, active_only=True)

    if request.method == 'POST':
        error = enforce_csrf_protection()
        if error:
            return error
        action = request.form.get('action', '')
        child_id = to_int(request.form.get('child_id', '0'), 0)
        link = None
        for lnk, child in children_rows:
            if child.id == child_id:
                link = lnk
                break
        if not link:
            flash('Ребенок не найден или не привязан к аккаунту.', 'error')
            return redirect('/parent/limits/')

        raw = json.loads(link.limits or '{}') if link.limits else {}
        blocked_ids = [int(x) for x in raw.get('blocked_dish_ids', []) if str(x).isdigit()]
        blocked_allergens = [str(a).strip().lower() for a in raw.get('blocked_allergens', []) if str(a).strip()]

        if action == 'add_dish':
            dish_id = to_int(request.form.get('dish_id', '0'), 0)
            dish = Dish.query.filter_by(id=dish_id, is_active=True).first() if dish_id > 0 else None
            if not dish:
                flash('Блюдо не найдено или недоступно.', 'error')
            elif dish_id not in blocked_ids:
                blocked_ids.append(dish_id)
                flash(f'Блюдо «{dish.title}» добавлено в список ограничений.', 'success')
            else:
                flash('Блюдо уже в списке ограничений.', 'error')

        elif action == 'add_allergen':
            allergen = request.form.get('allergen', '').strip().lower()
            if allergen not in KNOWN_ALLERGENS:
                flash('Неизвестный аллерген.', 'error')
            elif allergen not in blocked_allergens:
                blocked_allergens.append(allergen)
                flash(f'Аллерген «{allergen}» добавлен в список ограничений.', 'success')
            else:
                flash('Аллерген уже в списке.', 'error')
        else:
            flash('Неверное действие.', 'error')

        link.limits = json.dumps({'blocked_dish_ids': blocked_ids, 'blocked_allergens': blocked_allergens}, ensure_ascii=False)
        link.updated_at = datetime.utcnow()
        db.session.commit()
        return redirect('/parent/limits/')

    children_data = []
    for link, child in children_rows:
        raw = json.loads(link.limits or '{}') if link.limits else {}
        blocked_ids = [int(x) for x in raw.get('blocked_dish_ids', []) if str(x).isdigit()]
        blocked_allergens_list = [str(a).strip().lower() for a in raw.get('blocked_allergens', []) if str(a).strip()]
        dishes = Dish.query.filter(Dish.id.in_(blocked_ids)).all() if blocked_ids else []
        children_data.append({
            'link': link,
            'child': child,
            'child_name': build_child_display_name(child),
            'blocked_dishes': dishes,
            'blocked_allergens': blocked_allergens_list,
        })

    return render_template('parent_limits.html', **build_base_context(
        user,
        children_data=children_data,
        known_allergens=KNOWN_ALLERGENS,
    ))


@misc_bp.route('/parent/limits/remove/', methods=['POST'])
def parent_limits_remove():
    user, failure = require_roles({'parent'})
    if failure:
        return failure
    error = enforce_csrf_protection()
    if error:
        return error
    child_id = to_int(request.form.get('child_id', '0'), 0)
    remove_type = request.form.get('remove_type', '')
    remove_value = request.form.get('remove_value', '').strip()

    children_rows = get_parent_children_rows(user.id, active_only=True)
    link = None
    for lnk, child in children_rows:
        if child.id == child_id:
            link = lnk
            break
    if not link:
        flash('Ребенок не найден.', 'error')
        return redirect('/parent/limits/')

    raw = json.loads(link.limits or '{}') if link.limits else {}
    blocked_ids = [int(x) for x in raw.get('blocked_dish_ids', []) if str(x).isdigit()]
    blocked_allergens = [str(a).strip().lower() for a in raw.get('blocked_allergens', []) if str(a).strip()]

    if remove_type == 'dish':
        dish_id = to_int(remove_value, 0)
        if dish_id in blocked_ids:
            blocked_ids.remove(dish_id)
            flash('Ограничение на блюдо снято.', 'success')
    elif remove_type == 'allergen':
        allergen = remove_value.lower()
        if allergen in blocked_allergens:
            blocked_allergens.remove(allergen)
            flash(f'Аллерген «{allergen}» удалён из ограничений.', 'success')

    link.limits = json.dumps({'blocked_dish_ids': blocked_ids, 'blocked_allergens': blocked_allergens}, ensure_ascii=False)
    link.updated_at = datetime.utcnow()
    db.session.commit()
    return redirect('/parent/limits/')


misc = misc_bp
