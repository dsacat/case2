import base64
import csv
import io
from datetime import date, timedelta

import qrcode
from flask import Blueprint, jsonify, redirect, render_template, request, flash, make_response
from main import (
    db, build_base_context, require_user, message_page,
    is_valid_csrf_request,
    Users, Dish, MealOrder, PaymentOperation,
    build_orders_view, parse_order_status_label, create_notification,
    is_parent_of_student, build_child_display_name,
    to_int, get_cfg, datetime
)

orders_bp = Blueprint('orders', __name__)


def generate_qr_b64(url):
    img = qrcode.make(url)
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('ascii')


@orders_bp.route('/order/<int:order_id>/qr/')
def order_qr(order_id):
    user, failure = require_user(1)
    if failure:
        return failure
    order = MealOrder.query.get_or_404(order_id)
    if order.user_id != user.id:
        return message_page('Недостаточно прав.', user=user)
    url = request.host_url.rstrip('/') + f'/order/{order_id}/scan/'
    qr_b64 = generate_qr_b64(url)
    return render_template('qr.html', **build_base_context(user, order=order, dish=order.dish, qr_b64=qr_b64))


@orders_bp.route('/order/<int:order_id>/status.json')
def order_status_json(order_id):
    user, failure = require_user(1)
    if failure:
        return failure
    order = MealOrder.query.get_or_404(order_id)
    if order.user_id != user.id:
        return jsonify({'error': 'forbidden'}), 403
    return jsonify({'status': order.status})


@orders_bp.route('/order/<int:order_id>/received/', methods=['POST'])
def mark_order_received(order_id):
    user, failure = require_user(1)
    if failure:
        return failure
    if not is_valid_csrf_request():
        return message_page('Недействительный CSRF-токен.', user=user)
    order = MealOrder.query.get_or_404(order_id)
    if order.user_id != user.id:
        return message_page('Недостаточно прав.', user=user)
    if order.status in {'ordered', 'issued'}:
        order.status = 'received'
        order.received_at = datetime.utcnow()
        db.session.commit()
        flash('Получение отмечено.', 'success')
    return redirect('/profile/')


@orders_bp.route('/order/<int:order_id>/cancel/', methods=['POST'])
def cancel_order(order_id):
    user, failure = require_user(1)
    if failure:
        return failure
    if not is_valid_csrf_request():
        return message_page('Недействительный CSRF-токен.', user=user)
    order = MealOrder.query.get_or_404(order_id)
    if order.user_id != user.id and order.payer_user_id != user.id:
        return message_page('Недостаточно прав.', user=user)
    if order.status != 'ordered':
        flash('Нельзя отменить заказ со статусом «{}».'.format(order.status), 'error')
        return redirect('/orders/')
    refund = to_int(order.price, 0)
    payer_id = order.payer_user_id or order.user_id
    payer = db.session.get(Users, payer_id)
    if not payer:
        flash('Ошибка: плательщик не найден. Обратитесь к администратору.', 'error')
        return redirect('/orders/')
    payer.balance += refund
    db.session.add(PaymentOperation(
        user_id=payer_id,
        target_user_id=order.user_id,
        amount=refund,
        kind='order_cancel_refund',
        description=f'Возврат за отмену заказа',
    ))
    order.status = 'cancelled'
    db.session.commit()
    flash('Заказ отменён, средства возвращены на баланс.', 'success')
    return redirect('/orders/')


@orders_bp.route('/orders/')
def orders():
    user, failure = require_user(1)
    if failure:
        return failure

    status_filter = request.args.get('status', 'all')
    period_filter = request.args.get('period', 'all')
    orders_view, is_parent, status_filter, period_filter = build_orders_view(
        user,
        limit=240,
        status_filter=status_filter,
        period_filter=period_filter,
    )

    return render_template(
        'orders.html',
        **build_base_context(
            user,
            orders_view=orders_view,
            is_parent=is_parent,
            status_filter=status_filter,
            period_filter=period_filter,
            status_options=[
                ('all', 'Все статусы'),
                ('ordered', 'Заказано'),
                ('issued', 'Выдано'),
                ('received', 'Получено'),
                ('cancelled', 'Отменено'),
            ],
            period_options=[
                ('all', 'Все даты'),
                ('today', 'На сегодня'),
                ('future', 'Будущие'),
                ('past', 'Прошедшие'),
            ],
        ),
    )


@orders_bp.route('/orders/export.csv')
def export_orders_csv():
    user, failure = require_user(1)
    if failure:
        return failure
    rows = (
        db.session.query(MealOrder, Dish)
        .join(Dish, Dish.id == MealOrder.dish_id)
        .filter(MealOrder.user_id == user.id)
        .order_by(MealOrder.created_at.desc())
        .limit(1000)
        .all()
    )
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['ID', 'Дата заказа', 'Дата питания', 'Блюдо', 'Статус', 'Цена'])
    for order, dish in rows:
        writer.writerow([
            order.id,
            order.created_at.strftime('%d.%m.%Y %H:%M') if order.created_at else '',
            order.meal_date.strftime('%d.%m.%Y') if order.meal_date else '',
            dish.title,
            order.status,
            order.price,
        ])
    response = make_response(output.getvalue().encode('utf-8-sig'))
    response.headers['Content-Type'] = 'text/csv; charset=utf-8'
    response.headers['Content-Disposition'] = 'attachment; filename=orders.csv'
    return response


@orders_bp.route('/pay/', methods=['GET', 'POST'])
def pay():
    user, failure = require_user(1)
    if failure:
        return failure

    mes = ''
    form_data = {'sum': '', 'payment_type': 'top_up', 'subscription_days': '30'}
    field_errors = {}
    if request.method == 'POST':
        form_data['sum'] = request.form.get('sum', '')
        form_data['payment_type'] = request.form.get('payment_type', 'top_up')
        form_data['subscription_days'] = request.form.get('subscription_days', '30')
        amount = to_int(form_data['sum'], 0)
        payment_type = form_data['payment_type']
        if amount <= 0:
            mes = 'Введите корректную сумму.'
            field_errors['sum'] = True
        else:
            if payment_type == 'subscription':
                days = max(1, min(365, to_int(request.form.get('subscription_days', '30'), 30)))
                dop = user.dop_data or {}
                dop['subscription_until'] = (date.today() + timedelta(days=days)).isoformat()
                user.dop_data = dop
                kind = 'subscription'
                description = f'Оплата абонемента на {days} дн.'
            else:
                kind = 'top_up'
                description = 'Пополнение баланса'
            user.balance += amount
            db.session.add(PaymentOperation(user_id=user.id, target_user_id=user.id, amount=amount, kind=kind,
                                            description=description))
            create_notification(user.id, 'Баланс пополнен', f'+{amount} ₽', '/pay/')
            db.session.commit()
            mes = f'Баланс пополнен на {amount} ₽'

    operations = PaymentOperation.query.filter_by(user_id=user.id).order_by(PaymentOperation.created_at.desc()).limit(
        25).all()
    return render_template('pay.html', **build_base_context(user, mes=mes, operations=operations, form_data=form_data, field_errors=field_errors))


@orders_bp.route('/balance/topup/', methods=['GET', 'POST'])
def balance_topup():
    user, failure = require_user(1)
    if failure:
        return failure
    mes = ''
    mes_type = ''
    form_data = {'amount': '', 'comment': ''}
    field_errors = {}
    max_amount = to_int(get_cfg('topup_max_amount', 10000), 10000)
    if request.method == 'POST':
        form_data['amount'] = request.form.get('amount', '')
        form_data['comment'] = request.form.get('comment', '').strip()
        amount = to_int(form_data['amount'], 0)
        if amount <= 0:
            mes = 'Введите корректную сумму.'
            field_errors['amount'] = True
        elif amount > max_amount:
            mes = f'Максимальная сумма пополнения — {max_amount} ₽.'
            field_errors['amount'] = True
        else:
            description = 'Пополнение баланса'
            if form_data['comment']:
                description = f"Пополнение баланса: {form_data['comment'][:200]}"
            user.balance += amount
            db.session.add(PaymentOperation(user_id=user.id, target_user_id=user.id, amount=amount, kind='top_up', description=description))
            create_notification(user.id, 'Баланс пополнен', f'+{amount} ₽', '/balance/topup/')
            db.session.commit()
            mes = f'Баланс пополнен на {amount} ₽. Текущий баланс: {user.balance} ₽'
            mes_type = 'success'
            form_data = {'amount': '', 'comment': ''}
    return render_template('topup.html', **build_base_context(user, mes=mes, mes_type=mes_type, form_data=form_data, field_errors=field_errors, max_amount=max_amount))


@orders_bp.route('/dish/<int:dish_id>/preorder/', methods=['POST'])
def preorder_dish(dish_id):
    user, failure = require_user(1)
    if failure:
        return failure
    if not is_valid_csrf_request():
        return message_page('Недействительный CSRF-токен.', user=user)
    dish = Dish.query.get_or_404(dish_id)
    if not dish.is_active:
        return message_page('Блюдо недоступно.', user=user)
    if user.role not in {'student', 'parent'}:
        return message_page('Предзаказ доступен только школьнику или родителю.', user=user)

    target_user = user
    payer_user = user
    if user.role == 'parent':
        child_id = to_int(request.form.get('child_id', '0'), 0)
        if child_id <= 0:
            flash('Выберите ребенка для предзаказа.', 'error')
            return redirect(f'/dish/{dish_id}/')
        target_user = Users.query.filter_by(id=child_id, role='student', is_active=True).first()
        if not target_user or not is_parent_of_student(user.id, target_user.id):
            flash('Ребенок не привязан к вашему аккаунту.', 'error')
            return redirect(f'/dish/{dish_id}/')

    tomorrow = date.today() + timedelta(days=1)
    existing = MealOrder.query.filter_by(
        user_id=target_user.id,
        dish_id=dish_id,
        pre_order_date=tomorrow,
        status='ordered',
    ).first()
    if existing:
        flash('Предзаказ на это блюдо на завтра уже оформлен.', 'error')
        return redirect(f'/dish/{dish_id}/')
    price = to_int(dish.price, 0)
    if payer_user.balance < price:
        flash(f'Недостаточно средств для предзаказа. Баланс: {payer_user.balance} ₽, цена: {price} ₽.', 'error')
        return redirect(f'/dish/{dish_id}/')
    payer_user.balance -= price
    order = MealOrder(
        user_id=target_user.id,
        payer_user_id=payer_user.id,
        dish_id=dish_id,
        price=price,
        status='ordered',
        meal_date=tomorrow,
        pre_order_date=tomorrow,
    )
    db.session.add(order)
    db.session.add(PaymentOperation(
        user_id=payer_user.id,
        target_user_id=target_user.id,
        amount=-price,
        kind='preorder',
        description=f'Предзаказ: {dish.title} на {tomorrow.strftime("%d.%m.%Y")}',
    ))
    db.session.commit()
    if target_user.id == payer_user.id:
        flash(f'Предзаказ на «{dish.title}» оформлен на {tomorrow.strftime("%d.%m.%Y")}.', 'success')
    else:
        flash(f'Предзаказ на «{dish.title}» оформлен для {build_child_display_name(target_user)} на {tomorrow.strftime("%d.%m.%Y")}.', 'success')
    return redirect(f'/dish/{dish_id}/')


orders = orders_bp
