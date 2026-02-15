import csv
import io
import json
import re
import zipfile

from flask import Blueprint, redirect, render_template, request, flash, send_file
from main import (
    app, db, build_base_context, require_roles, require_user, message_page,
    is_valid_csrf_request,
    Users, Dish, MealOrder, InventoryItem, PurchaseRequest, Incident,
    build_report_payload, parse_order_status_label, create_notification, create_notification_for_roles,
    INCIDENT_KIND_LABELS, INCIDENT_SEVERITY_LABELS,
    is_role, is_any_role, role_level,
    to_int, to_float, to_date, datetime
)

kitchen = Blueprint('kitchen', __name__)


@kitchen.route('/kitchen/', methods=['GET', 'POST'])
def kitchen_page():
    user, failure = require_roles({'chef', 'admin', 'super_admin'})
    if failure:
        return failure

    if request.method == 'POST':
        if not is_valid_csrf_request():
            return message_page('Недействительный CSRF-токен.', user=user)
        action = request.form.get('action', '')
        if action in {'save_inventory', 'new_request', 'report_incident', 'issue_order'} and not is_role(user, 'chef'):
            return message_page('Недостаточно прав доступа.', user=user)
        if action == 'save_inventory':
            item_id = to_int(request.form.get('item_id', '0'), 0)
            name = request.form.get('name', '').strip()
            unit = request.form.get('unit', 'кг').strip() or 'кг'
            quantity = to_float(request.form.get('quantity', '0'), -1)
            min_quantity = to_float(request.form.get('min_quantity', '0'), -1)
            if name and quantity >= 0 and min_quantity >= 0:
                if item_id > 0:
                    item = db.session.get(InventoryItem, item_id)
                    if item:
                        item.name = name
                        item.unit = unit
                        item.quantity = quantity
                        item.min_quantity = min_quantity
                        item.updated_at = datetime.utcnow()
                else:
                    db.session.add(InventoryItem(name=name, unit=unit, quantity=quantity, min_quantity=min_quantity,
                                                 created_by=user.id, updated_at=datetime.utcnow()))
                db.session.commit()
                flash('Остаток сохранен.', 'success')
            else:
                flash('Проверьте поля остатка.', 'error')

        elif action == 'new_request':
            item_name = request.form.get('item_name', '').strip()
            quantity = to_float(request.form.get('quantity', '0'), -1)
            unit = request.form.get('unit', 'кг').strip() or 'кг'
            expected_cost = to_int(request.form.get('expected_cost', '0'), -1)
            comment = request.form.get('comment', '').strip()
            if item_name and quantity > 0 and expected_cost >= 0:
                db.session.add(PurchaseRequest(item_name=item_name, quantity=quantity, unit=unit,
                                               expected_cost=expected_cost, comment=comment,
                                               status='pending', created_by=user.id))
                db.session.commit()
                flash('Заявка на закупку отправлена.', 'success')
            else:
                flash('Проверьте поля заявки.', 'error')

        elif action == 'decision' and is_any_role(user, {'admin', 'super_admin'}):
            req_id = to_int(request.form.get('request_id', '0'), 0)
            decision = request.form.get('decision', '')
            req_obj = db.session.get(PurchaseRequest, req_id)
            if req_obj and req_obj.status == 'pending' and decision in {'approved', 'rejected'}:
                req_obj.status = decision
                req_obj.approved_by = user.id
                req_obj.resolved_at = datetime.utcnow()
                db.session.commit()
                create_notification(req_obj.created_by, 'Решение по заявке',
                                    f"Заявка {req_obj.item_name}: {'согласована' if decision == 'approved' else 'отклонена'}",
                                    '/kitchen/')
                db.session.commit()
                flash('Решение сохранено.', 'success')

        elif action == 'report_incident':
            title = request.form.get('incident_title', '').strip()
            description = request.form.get('incident_description', '').strip()
            kind = request.form.get('incident_kind', 'other').strip()
            severity = request.form.get('incident_severity', 'medium').strip()
            expected_date = to_date(request.form.get('incident_expected', '').strip())
            allowed_kinds = {'delay', 'spoilage', 'shortage', 'quality', 'other'}
            allowed_severity = {'low', 'medium', 'high', 'critical'}
            if kind not in allowed_kinds:
                kind = 'other'
            if severity not in allowed_severity:
                severity = 'medium'
            if title and description:
                db.session.add(Incident(
                    title=title,
                    description=description,
                    kind=kind,
                    severity=severity,
                    status='open',
                    expected_date=expected_date,
                    created_by=user.id,
                    updated_at=datetime.utcnow(),
                ))
                db.session.commit()
                create_notification_for_roles(
                    role_level('admin'),
                    'Инцидент на кухне',
                    f'{title} · Уровень: {severity}',
                    '/kitchen/',
                )
                db.session.commit()
                flash('Инцидент зафиксирован.', 'success')
            else:
                flash('Заполните тему и описание инцидента.', 'error')

        elif action == 'resolve_incident' and is_any_role(user, {'admin', 'super_admin'}):
            incident_id = to_int(request.form.get('incident_id', '0'), 0)
            incident = db.session.get(Incident, incident_id)
            if incident and incident.status == 'open':
                incident.status = 'resolved'
                incident.resolved_by = user.id
                incident.updated_at = datetime.utcnow()
                db.session.commit()
                flash('Инцидент закрыт.', 'success')

        elif action == 'issue_order' and is_role(user, 'chef'):
            order_id = to_int(request.form.get('order_id', '0'), 0)
            order = db.session.get(MealOrder, order_id)
            if order and order.status == 'ordered':
                order.status = 'issued'
                order.issued_at = datetime.utcnow()
                db.session.commit()
                create_notification(order.user_id, 'Блюдо выдано', 'Можно отметить получение в профиле.', '/profile/')
                db.session.commit()
                flash('Выдача отмечена.', 'success')

        return redirect('/kitchen/')

    inventory = InventoryItem.query.order_by((InventoryItem.quantity - InventoryItem.min_quantity).asc()).all()
    purchase_requests = PurchaseRequest.query.order_by(PurchaseRequest.created_at.desc()).limit(80).all()
    incidents = Incident.query.order_by(Incident.status.asc(), Incident.created_at.desc()).limit(80).all()
    orders = (
        db.session.query(MealOrder, Users, Dish)
        .join(Users, Users.id == MealOrder.user_id)
        .join(Dish, Dish.id == MealOrder.dish_id)
        .filter(MealOrder.status == 'ordered', MealOrder.pre_order_date.is_(None))
        .order_by(MealOrder.created_at.desc())
        .limit(50)
        .all()
    )

    _days_ru = {
        'Monday': 'Пн', 'Tuesday': 'Вт', 'Wednesday': 'Ср',
        'Thursday': 'Чт', 'Friday': 'Пт', 'Saturday': 'Сб', 'Sunday': 'Вс',
    }
    preorders_raw = (
        db.session.query(MealOrder, Users, Dish)
        .join(Users, Users.id == MealOrder.user_id)
        .join(Dish, Dish.id == MealOrder.dish_id)
        .filter(MealOrder.pre_order_date.isnot(None), MealOrder.status == 'ordered')
        .order_by(MealOrder.pre_order_date.asc(), MealOrder.created_at.asc())
        .limit(200)
        .all()
    )
    preorders_by_date = {}
    for order, student, dish in preorders_raw:
        day_ru = _days_ru.get(order.pre_order_date.strftime('%A'), '')
        key = order.pre_order_date.strftime('%d.%m.%Y') + f' ({day_ru})'
        if key not in preorders_by_date:
            preorders_by_date[key] = []
        preorders_by_date[key].append({
            'id': order.id,
            'dish_title': dish.title,
            'student_name': f'{student.surname} {student.name}'.strip(),
            'price': order.price,
        })

    return render_template('kitchen.html', **build_base_context(
        user,
        inventory=inventory,
        purchase_requests=purchase_requests,
        orders=orders,
        incidents=incidents,
        incident_kind_labels=INCIDENT_KIND_LABELS,
        incident_severity_labels=INCIDENT_SEVERITY_LABELS,
        can_manage_kitchen=is_role(user, 'chef'),
        can_approve=is_any_role(user, {'admin', 'super_admin'}),
        preorders_by_date=preorders_by_date,
    ))


@kitchen.route('/reports/')
def reports():
    user, failure = require_user(1)
    if failure:
        return failure
    payload = build_report_payload(user)
    return render_template('reports.html', **build_base_context(user, report=payload,
                                                                charts_json=json.dumps(payload['charts'],
                                                                                       ensure_ascii=False)))


@kitchen.route('/reports/export.zip')
def reports_export():
    user, failure = require_user(1)
    if failure:
        return failure
    payload = build_report_payload(user)
    archive_io = io.BytesIO()
    with zipfile.ZipFile(archive_io, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        meta = {
            'title': payload.get('title', ''),
            'subtitle': payload.get('subtitle', ''),
            'created_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'user_id': user.id,
            'role': user.role,
        }
        zf.writestr('summary.json', json.dumps(meta, ensure_ascii=False, indent=2).encode('utf-8'))
        for idx, table in enumerate(payload.get('tables', []), start=1):
            output = io.StringIO()
            writer = csv.writer(output, delimiter=';')
            columns = table.get('columns', [])
            keys = table.get('keys', [])
            writer.writerow(columns)
            for row in table.get('rows', []):
                writer.writerow([str(row.get(key, '')) for key in keys])
            safe_title = re.sub(r'[^A-Za-z0-9_\\-]+', '_', str(table.get('title', f'table_{idx}'))).strip('_').lower()
            if not safe_title:
                safe_title = f'table_{idx}'
            zf.writestr(f'{idx:02d}_{safe_title}.csv', output.getvalue().encode('utf-8-sig'))
    archive_io.seek(0)
    download_name = f"reports_{user.role}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip"
    return send_file(archive_io, mimetype='application/zip', as_attachment=True, download_name=download_name)



@kitchen.route('/order/<int:order_id>/scan/', methods=['GET'])
def order_scan_get(order_id):
    user, failure = require_roles({'chef', 'admin', 'super_admin'})
    if failure:
        return failure
    order = MealOrder.query.get_or_404(order_id)
    return render_template('scan.html', **build_base_context(
        user,
        order=order,
        dish=order.dish,
        student=order.user,
        issued=(order.status in {'issued', 'received'}),
    ))


@kitchen.route('/order/<int:order_id>/scan/', methods=['POST'])
def order_scan_post(order_id):
    user, failure = require_roles({'chef', 'admin', 'super_admin'})
    if failure:
        return failure
    if not is_valid_csrf_request():
        return message_page('Недействительный CSRF-токен.', user=user)
    order = MealOrder.query.get_or_404(order_id)
    if order.status == 'ordered':
        order.status = 'issued'
        order.issued_at = datetime.utcnow()
        db.session.commit()
        create_notification(
            order.user_id,
            'Заказ выдан',
            f'Ваш заказ «{order.dish.title}» выдан. Заберите у стойки.',
            f'/order/{order_id}/qr/',
        )
        db.session.commit()
    return render_template('scan.html', **build_base_context(
        user,
        order=order,
        dish=order.dish,
        student=order.user,
        issued=(order.status in {'issued', 'received'}),
    ))


@kitchen.route('/kitchen/scan/')
def kitchen_scan_page():
    user, failure = require_roles({'chef', 'admin', 'super_admin'})
    if failure:
        return failure
    return render_template('kitchen_scan.html', **build_base_context(user))

