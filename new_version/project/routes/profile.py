import time
from datetime import date, timedelta

from flask import Blueprint, g, redirect, render_template, request, flash, jsonify, make_response, session
from main import (
    app, db, build_base_context, require_user, require_roles, message_page,
    Users, Session, Dish, MealOrder, PaymentOperation, ParentInvite,
    get_parent_children_rows, get_student_parent_rows, get_parent_child_link,
    build_child_display_name, ensure_parent_student_link, mark_parent_invite_used,
    generate_parent_invite, build_parent_invite_url,
    get_notification_preferences, get_email_notifications_enabled,
    parse_order_status_label, get_student_daily_spent, create_notification,
    normalize_rule_tokens, stringify_rule_tokens,
    has_permission, role_level, is_role,
    to_int, func, datetime, ICON_DIR,
    save_as_avif, generate_password_hash, check_password_hash, session_cache,
    Image,
    PendingPasswordChange, create_pending_password_change, apply_pending_password_change,
    send_email, get_cfg,
    is_valid_csrf_request,
)

profile_bp = Blueprint('profile', __name__)


@profile_bp.route('/profile/', methods=['GET', 'POST'])
def profile():
    user, failure = require_user(1)
    if failure:
        return failure

    if request.method == 'POST':
        action = request.form.get('action', 'save_profile').strip()

        if action == 'save_profile':
            user.name = request.form.get('user_name', user.name).strip() or user.name
            user.surname = request.form.get('user_surname', user.surname).strip() or user.surname
            user.otchestvo = request.form.get('user_patronymic', user.otchestvo).strip()
            dop = user.dop_data or {}
            dop['allergies'] = request.form.get('allergies', '').strip()
            dop['preferences'] = request.form.get('preferences', '').strip()
            dop['notify_orders'] = bool(request.form.get('notify_orders'))
            dop['notify_payments'] = bool(request.form.get('notify_payments'))
            dop['notify_feedback'] = bool(request.form.get('notify_feedback'))
            dop['notify_kitchen'] = bool(request.form.get('notify_kitchen'))
            dop['notify_system'] = bool(request.form.get('notify_system'))
            dop['email_notifications'] = bool(request.form.get('email_notifications'))
            user.dop_data = dop
            db.session.commit()
            flash('Профиль обновлен.', 'success')
            return redirect('/profile/')

        if action == 'create_family_invite':
            if user.role != 'student':
                flash('Только школьник может создавать коды привязки.', 'error')
                return redirect('/profile/')
            ttl_hours = max(1, min(168, to_int(request.form.get('ttl_hours', '72'), 72)))
            invite = generate_parent_invite(user.id, ttl_hours=ttl_hours)
            invite_link = build_parent_invite_url(invite.token)
            flash(f'Код для родителя: {invite.code}. Ссылка: {invite_link}', 'success')
            return redirect('/profile/')

        if action == 'link_child_by_code':
            if user.role != 'parent':
                flash('Только родитель может привязывать ребенка.', 'error')
                return redirect('/profile/')
            code = str(request.form.get('invite_code', '')).strip().upper()
            if len(code) < 4:
                flash('Укажите корректный код.', 'error')
                return redirect('/profile/')
            invite = (
                ParentInvite.query
                .join(Users, Users.id == ParentInvite.student_id)
                .filter(
                    ParentInvite.code == code,
                    ParentInvite.is_used.is_(False),
                    ParentInvite.expires_at > datetime.utcnow(),
                    Users.role == 'student',
                    Users.is_active.is_(True),
                )
                .first()
            )
            if not invite:
                flash('Код недействителен или истек.', 'error')
                return redirect('/profile/')
            ensure_parent_student_link(user.id, invite.student_id)
            mark_parent_invite_used(invite, user.id)
            student = db.session.get(Users, invite.student_id)
            student_name = build_child_display_name(student) if student else 'Ученик'
            parent_name = build_child_display_name(user)
            create_notification(user.id, 'Ребенок привязан', f'Привязан ученик: {student_name}.', '/profile/')
            create_notification(invite.student_id, 'Подключен родитель',
                                f'{parent_name} получил доступ к вашему питанию.', '/profile/')
            db.session.commit()
            flash('Ребенок успешно привязан по коду.', 'success')
            return redirect('/profile/')

        if action == 'update_child_rules':
            if user.role != 'parent':
                flash('Недостаточно прав.', 'error')
                return redirect('/profile/')
            child_id = to_int(request.form.get('child_id', '0'), 0)
            link = get_parent_child_link(user.id, child_id, active_only=False)
            if not link:
                flash('Связка с ребенком не найдена.', 'error')
                return redirect('/profile/')
            link.is_active = bool(request.form.get('link_active'))
            link.daily_limit = max(0, min(50000, to_int(request.form.get('daily_limit', '0'), 0)))
            link.allowed_products = stringify_rule_tokens(
                normalize_rule_tokens(request.form.get('allowed_products', '')))
            link.required_products = stringify_rule_tokens(
                normalize_rule_tokens(request.form.get('required_products', '')))
            link.forbidden_products = stringify_rule_tokens(
                normalize_rule_tokens(request.form.get('forbidden_products', '')))
            link.updated_at = datetime.utcnow()
            db.session.commit()
            flash('Ограничения обновлены.', 'success')
            return redirect('/profile/')

        if action == 'unlink_child':
            if user.role != 'parent':
                flash('Недостаточно прав.', 'error')
                return redirect('/profile/')
            child_id = to_int(request.form.get('child_id', '0'), 0)
            link = get_parent_child_link(user.id, child_id, active_only=False)
            if not link:
                flash('Связка не найдена.', 'error')
                return redirect('/profile/')
            link.is_active = False
            link.updated_at = datetime.utcnow()
            db.session.commit()
            flash('Ребенок отвязан.', 'success')
            return redirect('/profile/')

        if action == 'change_password':
            current_password = request.form.get('current_password', '').strip()
            new_password = request.form.get('new_password', '').strip()
            confirm_password = request.form.get('confirm_password', '').strip()

            if not current_password:
                flash('Укажите текущий пароль.', 'error')
                return redirect('/profile/')

            if not check_password_hash(user.psw, current_password):
                app.logger.warning(f'Failed password change attempt for user {user.id}')
                flash('Неверный текущий пароль.', 'error')
                return redirect('/profile/')

            if len(new_password) < 6:
                flash('Новый пароль должен быть не менее 6 символов.', 'error')
                return redirect('/profile/')

            if new_password != confirm_password:
                flash('Пароли не совпадают.', 'error')
                return redirect('/profile/')

            if new_password == current_password:
                flash('Новый пароль совпадает с текущим.', 'error')
                return redirect('/profile/')

            new_hash = generate_password_hash(new_password)
            create_pending_password_change(user, new_hash)
            flash('Письмо с подтверждением отправлено на вашу почту. Перейдите по ссылке из письма для применения нового пароля.', 'success')
            return redirect('/profile/')

        if action == 'close_session':
            session_id = to_int(request.form.get('session_id', '0'), 0)
            if session_id == 0:
                flash('Некорректный ID сессии.', 'error')
                return redirect('/profile/')

            session_obj = Session.query.filter_by(id=session_id, user_id=user.id).first()
            if not session_obj:
                flash('Сессия не найдена.', 'error')
                return redirect('/profile/')

            current_token = request.cookies.get('session_token')
            if session_obj.token == current_token:
                flash('Невозможно закрыть текущую сессию. Используйте выход (logout).', 'error')
                return redirect('/profile/')

            session_obj.is_active = False
            db.session.commit()
            app.logger.info(f'Session {session_id} closed by user {user.id}')
            flash('Сессия закрыта.', 'success')
            return redirect('/profile/')

        if action == 'close_all_sessions':
            current_token = request.cookies.get('session_token')
            sessions = Session.query.filter_by(user_id=user.id, is_active=True).all()
            closed_count = 0
            for session_obj in sessions:
                if session_obj.token != current_token:
                    session_obj.is_active = False
                    closed_count += 1
            db.session.commit()
            app.logger.info(f'User {user.id} closed {closed_count} sessions')
            flash(f'Закрыто {closed_count} сессий.', 'success')
            return redirect('/profile/')

    is_parent = user.role == 'parent'
    is_student = user.role == 'student'
    since_30 = datetime.utcnow() - timedelta(days=30)

    orders_view = []
    linked_children = []
    linked_parents = []
    family_invites = []
    family_total_spent_30 = 0
    family_total_spent_all = 0

    if is_parent:
        children_rows = get_parent_children_rows(user.id, active_only=False)
        active_child_ids = [child.id for link, child in children_rows if link.is_active]
        query_ids = active_child_ids if active_child_ids else [-1]

        order_rows = (
            db.session.query(MealOrder, Dish, Users)
            .join(Dish, Dish.id == MealOrder.dish_id)
            .join(Users, Users.id == MealOrder.user_id)
            .filter(MealOrder.user_id.in_(query_ids))
            .order_by(MealOrder.created_at.desc())
            .limit(40)
            .all()
        )
        for order, dish, student_user in order_rows:
            orders_view.append({
                'id': order.id,
                'date': order.created_at.strftime('%d.%m.%Y %H:%M'),
                'meal_date': order.meal_date.strftime('%d.%m.%Y') if order.meal_date else '-',
                'dish': dish.title,
                'status': parse_order_status_label(order.status),
                'price': order.price,
                'child': build_child_display_name(student_user),
                'can_received': False,
            })

        if active_child_ids:
            family_total_spent_30 = to_int(
                db.session.query(func.coalesce(func.sum(MealOrder.price), 0))
                .filter(MealOrder.user_id.in_(active_child_ids), MealOrder.created_at >= since_30)
                .scalar(),
                0,
            )
            family_total_spent_all = to_int(
                db.session.query(func.coalesce(func.sum(MealOrder.price), 0))
                .filter(MealOrder.user_id.in_(active_child_ids))
                .scalar(),
                0,
            )

        for link, child in children_rows:
            spent_today = get_student_daily_spent(child.id, date.today()) if link.is_active else 0
            spent_30 = to_int(
                db.session.query(func.coalesce(func.sum(MealOrder.price), 0))
                .filter(MealOrder.user_id == child.id, MealOrder.created_at >= since_30)
                .scalar(),
                0,
            )
            spent_all = to_int(
                db.session.query(func.coalesce(func.sum(MealOrder.price), 0))
                .filter(MealOrder.user_id == child.id)
                .scalar(),
                0,
            )
            linked_children.append({
                'id': child.id,
                'name': build_child_display_name(child),
                'is_active': bool(link.is_active),
                'daily_limit': to_int(link.daily_limit, 0),
                'allowed_products': str(link.allowed_products or ''),
                'required_products': str(link.required_products or ''),
                'forbidden_products': str(link.forbidden_products or ''),
                'spent_today': spent_today,
                'spent_30': spent_30,
                'spent_all': spent_all,
            })
    else:
        active_orders = (
            db.session.query(MealOrder, Dish)
            .join(Dish, Dish.id == MealOrder.dish_id)
            .filter(MealOrder.user_id == user.id)
            .order_by(MealOrder.created_at.desc())
            .limit(40)
            .all()
        )
        for order, dish in active_orders:
            orders_view.append({
                'id': order.id,
                'date': order.created_at.strftime('%d.%m.%Y %H:%M'),
                'meal_date': order.meal_date.strftime('%d.%m.%Y') if order.meal_date else '-',
                'dish': dish.title,
                'status': parse_order_status_label(order.status),
                'price': order.price,
                'child': '',
                'can_received': order.status in {'ordered', 'issued'},
            })

    if is_student:
        linked_parents = [
            {
                'id': parent.id,
                'name': build_child_display_name(parent),
            }
            for _, parent in get_student_parent_rows(user.id, active_only=True)
        ]
        invite_rows = (
            ParentInvite.query
            .filter(
                ParentInvite.student_id == user.id,
                ParentInvite.is_used.is_(False),
                ParentInvite.expires_at > datetime.utcnow(),
            )
            .order_by(ParentInvite.created_at.desc())
            .limit(8)
            .all()
        )
        for invite in invite_rows:
            family_invites.append({
                'code': invite.code,
                'expires': invite.expires_at.strftime('%d.%m.%Y %H:%M'),
                'link': build_parent_invite_url(invite.token),
            })

    def detect_tx_type(tx):
        desc = str(tx.get('description', '')).lower()
        amt = tx.get('amount', 0)
        if amt < 0:
            return 'order'
        if 'возврат' in desc or 'refund' in desc or 'отмен' in desc:
            return 'refund'
        return 'deposit'

    TX_PER_PAGE = 20
    tx_page = max(1, to_int(request.args.get('page', 1)))
    tx_all_rows = (
        PaymentOperation.query
        .filter_by(user_id=user.id)
        .order_by(PaymentOperation.created_at.desc())
        .all()
    )
    tx_all = [
        {
            'date': tx.created_at.strftime('%d.%m.%Y %H:%M') if tx.created_at else '-',
            'amount': tx.amount,
            'description': tx.description or tx.kind,
        }
        for tx in tx_all_rows
    ]
    for t in tx_all:
        t['tx_type'] = detect_tx_type(t)
    tx_total = len(tx_all)
    tx_pages = max(1, (tx_total + TX_PER_PAGE - 1) // TX_PER_PAGE)
    tx_page = min(tx_page, tx_pages)
    tx_start = (tx_page - 1) * TX_PER_PAGE
    transactions = tx_all[tx_start:tx_start + TX_PER_PAGE]

    fav_ids = [int(x) for x in ((user.dop_data or {}).get('favorites') or []) if str(x).isdigit()]
    fav_dishes = Dish.query.filter(Dish.id.in_(fav_ids), Dish.is_active == True).all() if fav_ids else []

    meal_stats_rows = (
        db.session.query(Dish.title, func.count(MealOrder.id).label('cnt'))
        .join(MealOrder, MealOrder.dish_id == Dish.id)
        .filter(MealOrder.user_id == user.id, MealOrder.status != 'cancelled')
        .group_by(Dish.id)
        .order_by(func.count(MealOrder.id).desc())
        .limit(3)
        .all()
    )
    total_orders_count = MealOrder.query.filter(
        MealOrder.user_id == user.id, MealOrder.status != 'cancelled'
    ).count()
    total_spent = db.session.query(func.sum(MealOrder.price)).filter(
        MealOrder.user_id == user.id, MealOrder.status != 'cancelled'
    ).scalar() or 0
    meal_stats = {
        'total_orders': total_orders_count,
        'total_spent': total_spent,
        'top_dishes': [{'title': t, 'count': c} for t, c in meal_stats_rows],
    }

    current_token = request.cookies.get('session_token')
    user_sessions = []
    sessions_rows = Session.query.filter_by(user_id=user.id, is_active=True).order_by(Session.last_seen.desc()).all()
    for sess in sessions_rows:
        is_current = sess.token == current_token
        user_sessions.append({
            'id': sess.id,
            'user_agent': sess.user_agent[:80] if sess.user_agent else '-',
            'ip_address': sess.ip_address or '-',
            'created': sess.created_at.strftime('%d.%m.%Y %H:%M') if sess.created_at else '-',
            'last_seen': sess.last_seen.strftime('%d.%m.%Y %H:%M') if sess.last_seen else '-',
            'is_current': is_current,
        })

    preorders_raw = (
        db.session.query(MealOrder, Dish)
        .join(Dish, Dish.id == MealOrder.dish_id)
        .filter(
            MealOrder.user_id == user.id,
            MealOrder.pre_order_date.isnot(None),
            MealOrder.status == 'ordered',
        )
        .order_by(MealOrder.pre_order_date.asc())
        .limit(50)
        .all()
    )
    preorders_view = [
        {
            'id': order.id,
            'dish_title': dish.title,
            'pre_order_date': order.pre_order_date.strftime('%d.%m.%Y'),
            'price': order.price,
            'status': order.status,
        }
        for order, dish in preorders_raw
    ]

    return render_template('profile.html', **build_base_context(
        user,
        orders_view=orders_view,
        allergies=(user.dop_data or {}).get('allergies', ''),
        preferences=(user.dop_data or {}).get('preferences', ''),
        notify_prefs=get_notification_preferences(user),
        email_notifications=get_email_notifications_enabled(user),
        can_open_kitchen=is_role(user, 'chef'),
        can_open_settings=has_permission(user, role_level('admin')),
        can_open_admin_console=is_role(user, 'super_admin'),
        is_parent=is_parent,
        is_student=is_student,
        linked_children=linked_children,
        linked_parents=linked_parents,
        family_invites=family_invites,
        family_total_spent_30=family_total_spent_30,
        family_total_spent_all=family_total_spent_all,
        user_sessions=user_sessions,
        transactions=transactions,
        tx_page=tx_page,
        tx_pages=tx_pages,
        tx_total=tx_total,
        meal_stats=meal_stats,
        fav_dishes=fav_dishes,
        allergen_filter_list=(user.dop_data or {}).get('allergens', []),
        preorders_view=preorders_view,
    ))


@profile_bp.route('/profile/allergens/', methods=['POST'])
def save_allergens():
    user, failure = require_user(1)
    if failure:
        return jsonify({'ok': False, 'error': 'not_authenticated'}), 401
    from main import get_csrf_token
    token_in_header = request.headers.get('X-CSRFToken', '')
    expected = get_csrf_token()
    if not token_in_header or token_in_header != expected:
        return jsonify({'ok': False, 'error': 'csrf'}), 403
    data = request.get_json(silent=True) or {}
    allergens = data.get('allergens', [])
    valid_keys = {'глютен', 'лактоза', 'орехи', 'яйца', 'рыба', 'соя', 'кунжут'}
    allergens = [a for a in allergens if isinstance(a, str) and a in valid_keys]
    dop = dict(user.dop_data or {})
    dop['allergens'] = allergens
    user.dop_data = dop
    db.session.commit()
    return jsonify({'ok': True})


@profile_bp.route('/password/change/confirm/<token>/')
def confirm_password_change(token):
    user, error = apply_pending_password_change(token)
    if error:
        return message_page(error, redirect_to='/login/new/')
    return render_template('password_change_confirmed.html', **build_base_context(None))


@profile_bp.route('/family/link/<token>/')
def link_child_by_token(token):
    user, failure = require_roles({'parent'})
    if failure:
        return failure

    invite = (
        ParentInvite.query
        .join(Users, Users.id == ParentInvite.student_id)
        .filter(
            ParentInvite.token == token,
            ParentInvite.is_used.is_(False),
            ParentInvite.expires_at > datetime.utcnow(),
            Users.role == 'student',
            Users.is_active.is_(True),
        )
        .first()
    )
    if not invite:
        flash('Ссылка недействительна или истекла.', 'error')
        return redirect('/profile/')

    ensure_parent_student_link(user.id, invite.student_id)
    mark_parent_invite_used(invite, user.id)
    student = db.session.get(Users, invite.student_id)
    student_name = build_child_display_name(student) if student else 'Ученик'
    parent_name = build_child_display_name(user)
    create_notification(user.id, 'Ребенок привязан', f'Привязан ученик: {student_name}.', '/profile/')
    create_notification(invite.student_id, 'Подключен родитель',
                        f'{parent_name} получил доступ к вашему питанию.', '/profile/')
    db.session.commit()
    flash('Ребенок успешно привязан по ссылке.', 'success')
    return redirect('/profile/')


@profile_bp.route('/upload_avatar/', methods=['POST'])
def upload_avatar():
    user, failure = require_user(1)
    if failure:
        return jsonify({'status': 'error', 'message': 'Требуется авторизация'}), 401

    from main import is_valid_csrf_request
    if not is_valid_csrf_request():
        return jsonify({'status': 'error', 'message': 'CSRF token invalid'}), 403

    if 'file' not in request.files:
        return jsonify({'status': 'error', 'message': 'Файл не выбран'}), 400

    file = request.files['file']
    if not file or not file.filename:
        return jsonify({'status': 'error', 'message': 'Файл не выбран'}), 400

    file.seek(0, 2)
    file_size = file.tell()
    file.seek(0)
    if file_size > 2 * 1024 * 1024:
        return jsonify({'status': 'error', 'message': 'Файл слишком большой (макс 2МБ)'}), 400

    try:
        image = Image.open(file)

        if image.format and image.format.upper() not in {'JPEG', 'PNG', 'WEBP'}:
            return jsonify({'status': 'error', 'message': 'Неподдерживаемый формат файла. Допускаются JPG, PNG, WEBP'}), 400

        image = image.convert('RGBA')
        width, height = image.size

        if width < 100 or height < 100:
            return jsonify({'status': 'error', 'message': 'Изображение слишком маленькое (минимум 100x100)'}), 400
        if width > 10000 or height > 10000:
            return jsonify({'status': 'error', 'message': 'Изображение слишком большое'}), 400

        side = min(width, height)
        image = image.crop(((width - side) / 2, (height - side) / 2, (width + side) / 2, (height + side) / 2))
        image = image.resize((512, 512), Image.LANCZOS)
        old_avatar_path = ICON_DIR / f'{user.id}.avif'
        if old_avatar_path.exists():
            old_avatar_path.unlink()
        save_as_avif(image, ICON_DIR / f'{user.id}.avif')
        if not user.icon:
            user.icon = True
            db.session.commit()
        return jsonify({'status': 'success'})

    except IOError:
        app.logger.warning(f'Invalid image file uploaded by user {user.id}')
        return jsonify({'status': 'error', 'message': 'Невалидное изображение'}), 400
    except Exception as exc:
        app.logger.error(f'upload_avatar error for user {user.id}: {str(exc)}')
        return jsonify({'status': 'error', 'message': 'Ошибка при обработке изображения'}), 500


@profile_bp.route('/del_ava/', methods=['POST'])
def del_ava():
    user, failure = require_user(1)
    if failure:
        return failure
    if not is_valid_csrf_request():
        return message_page('Недействительный CSRF-токен.', user=user)
    if user.icon:
        avatar_path = ICON_DIR / f'{user.id}.avif'
        if avatar_path.exists():
            avatar_path.unlink()
        user.icon = False
        db.session.commit()
    return redirect('/profile/')


@profile_bp.route('/del_account/', methods=['GET', 'POST'])
def del_account():
    user, failure = require_user(1)
    if failure:
        return failure
    if request.method == 'POST':
        if request.form.get('confirm', '').strip().upper() != 'DELETE':
            return render_template('del_account.html',
                                   **build_base_context(user, mes='Введите DELETE для подтверждения.'))
        Session.query.filter_by(user_id=user.id).update({'is_active': False})
        for token, payload in list(session_cache.items()):
            if payload.get('user_id') == user.id:
                session_cache.pop(token, None)
        if user.icon:
            avatar_path = ICON_DIR / f'{user.id}.avif'
            if avatar_path.exists():
                avatar_path.unlink()
        user.is_active = False
        user.email = f'deleted_{user.id}_{int(time.time())}@local'
        user.name = 'Удалено'
        user.surname = 'Пользователь'
        user.otchestvo = ''
        user.balance = 0
        db.session.commit()
        response = make_response(redirect('/'))
        response.set_cookie('session_token', '', expires=0)
        return response
    return render_template('del_account.html', **build_base_context(user, mes=''))


profile = profile_bp
