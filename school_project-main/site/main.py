import csv
import getpass
import io
import json
import logging
import os
import re
import secrets
import sys
import time
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from random import choices
from threading import Thread

from flask import Flask, flash, g, has_request_context, jsonify, make_response, redirect, render_template, request, \
    send_file, session
from flask_mail import Mail, Message
from flask_sqlalchemy import SQLAlchemy
from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageOps
from sqlalchemy import UniqueConstraint, func, inspect, text
from werkzeug.security import check_password_hash, generate_password_hash

from custom_console import CustomConsole

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / 'static'
TEMPLATES_DIR = BASE_DIR / 'templates'
ICON_DIR = STATIC_DIR / 'icons'
DISH_ICON_DIR = ICON_DIR / 'dishes'
BG_DIR = STATIC_DIR / 'bg'
DATA_DIR = BASE_DIR / 'data'
SECRET_KEY_FILE = BASE_DIR / '.secret_key'
ENV_SECRET_KEY_NAMES = ('SMART_CANTEEN_SECRET_KEY', 'SECRET_KEY')


def read_secret_key_file():
    try:
        if SECRET_KEY_FILE.exists():
            return SECRET_KEY_FILE.read_text(encoding='utf-8').strip()
    except OSError:
        return ''
    return ''


def write_secret_key_file(secret_key):
    try:
        SECRET_KEY_FILE.write_text(f'{secret_key}\n', encoding='utf-8')
        return True
    except OSError:
        return False


def resolve_secret_key(cfg_secret_key=''):
    for env_name in ENV_SECRET_KEY_NAMES:
        value = os.environ.get(env_name, '').strip()
        if value:
            return value, f'env:{env_name}'

    file_secret_key = read_secret_key_file()
    if file_secret_key:
        return file_secret_key, 'file'

    cfg_secret_key = str(cfg_secret_key or '').strip()
    if cfg_secret_key:
        return cfg_secret_key, 'cfg'

    generated_key = secrets.token_urlsafe(64)
    write_secret_key_file(generated_key)
    return generated_key, 'generated'

symbols = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'

app = Flask(__name__, static_folder=str(STATIC_DIR), template_folder=str(TEMPLATES_DIR))
DATA_DIR.mkdir(parents=True, exist_ok=True)
app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{(DATA_DIR / 'DB.db').as_posix()}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['UPLOAD_FOLDER'] = str(ICON_DIR)
app.config['MAX_CONTENT_LENGTH'] = 8 * 1024 * 1024
bootstrap_secret_key, _ = resolve_secret_key()
app.config['SECRET_KEY'] = bootstrap_secret_key
app.secret_key = bootstrap_secret_key

for folder in [STATIC_DIR, ICON_DIR, DISH_ICON_DIR, BG_DIR, STATIC_DIR / 'ico']:
    folder.mkdir(parents=True, exist_ok=True)

USER_ROLES = {
    'student': {'level': 1, 'session_hours': 24, 'label': 'Ученик'},
    'parent': {'level': 2, 'session_hours': 12, 'label': 'Родитель'},
    'moder': {'level': 3, 'session_hours': 8, 'label': 'Модератор'},
    'chef': {'level': 4, 'session_hours': 8, 'label': 'Повар'},
    'admin': {'level': 5, 'session_hours': 6, 'label': 'Администратор'},
    'super_admin': {'level': 6, 'session_hours': 5, 'label': 'Супер-администратор'},
}

REGISTRATION_ROLES = ['student', 'parent']

DEFAULT_CFG = {
    'Name': 'Smart Canteen',
    'Name_sch': 'вашей школы',
    'adress': '127.0.0.1',
    'port': 5000,
    'debug': False,
    'protection': 64,
    'mail_enabled': False,
    'mail_server': '',
    'mail_port': 587,
    'mail_username': '',
    'mail_password': '',
    'mail_use_tls': True,
    'gen_admin': 'admin@school.local',
    'contact_data': [
        [['Школьная столовая'], ['Поддержка'], ['Режим работы']],
        [['+7 (900) 000-00-00'], ['support@school.local'], ['Пн-Пт 08:00-18:00']],
    ],
    'ico_path': 'ico/icon.ico',
    'ico_path_light': 'ico/icon.ico',
    'ico_path_dark': 'ico/icon.ico',
    'ico_png_path': 'ico/site_favicon_16.png',
    'bg_path': 'bg/bg.avif',
    'bg_path_light': 'bg/bg.avif',
    'bg_path_dark': 'bg/bg.avif',
    'assets_rev': 1,
    'setup_done': False,
    'setup_access_code_hash': '',
    'setup_access_mode': '',
    'setup_access_hint': '',
    'setup_access_issued_at': '',
    'secret_key': '',
    'super_admin_password_hash': '',
}

INCIDENT_KIND_LABELS = {
    'delay': 'Задержка поставки',
    'spoilage': 'Порча / стухло',
    'shortage': 'Нехватка продуктов',
    'quality': 'Качество не соответствует',
    'other': 'Другое',
}

INCIDENT_SEVERITY_LABELS = {
    'low': 'Низкий',
    'medium': 'Средний',
    'high': 'Высокий',
    'critical': 'Критический',
}

db = SQLAlchemy(app)
mail = Mail(app)

session_cache = {}
MAX_CACHE_SIZE = 2000
admin_console_runner = None
admin_console_runner_admin = None

ADMIN_CONSOLE_ALLOWED_COMMANDS = {
    'help',
    'clear',
    'list_users',
    'user_info',
    'sessions',
    'stats',
    'role_stats',
    'recent_logins',
    'list_dishes',
    'feedback_stats',
    'list_feedback',
    'list_purchase_requests',
    'system_info',
    'list_inventory',
    'inventory_stats',
    'list_incidents',
    'incident_stats',
    'list_orders',
    'order_stats',
    'list_payments',
    'payment_stats',
    'list_notifications',
    'notification_stats',
    'list_reviews',
    'review_stats',
}

CONSOLE_COMMAND_SPECS = {
    'help': {'title': 'Справка', 'args': [], 'help': 'Показать список команд'},
    'clear': {'title': 'Очистить лог', 'args': [], 'help': 'Очистить окно веб-консоли'},
    'get_cfg': {'title': 'Получить настройку', 'args': ['name'], 'help': 'Прочитать значение CFG'},
    'set_cfg': {'title': 'Изменить настройку', 'args': ['name', 'value'], 'help': 'Записать значение CFG'},
    'list_users': {'title': 'Список пользователей', 'args': ['limit'], 'help': 'Например: 100'},
    'user_info': {'title': 'Карточка пользователя', 'args': ['user_id'], 'help': 'ID пользователя'},
    'change_role': {'title': 'Сменить роль', 'args': ['user_id', 'role'],
                    'help': 'role: student/parent/moder/chef/admin/super_admin'},
    'activate_user': {'title': 'Активировать пользователя', 'args': ['user_id'], 'help': 'Включить аккаунт'},
    'deactivate_user': {'title': 'Деактивировать пользователя', 'args': ['user_id'], 'help': 'Отключить аккаунт'},
    'delete_user': {'title': 'Обезличить пользователя', 'args': ['user_id'], 'help': 'Мягкое удаление'},
    'sessions': {'title': 'Активные сессии', 'args': [], 'help': 'Список текущих сессий'},
    'kill_session': {'title': 'Завершить сессию', 'args': ['session_id'], 'help': 'ID сессии'},
    'stats': {'title': 'Сводная статистика', 'args': [], 'help': 'Общий срез проекта'},
    'role_stats': {'title': 'Статистика ролей', 'args': [], 'help': 'Количество пользователей по ролям'},
    'recent_logins': {'title': 'Последние входы', 'args': ['limit'], 'help': 'Например: 20'},
    'list_dishes': {'title': 'Список блюд', 'args': ['limit'], 'help': 'Например: 100'},
    'delete_dish': {'title': 'Скрыть блюдо', 'args': ['dish_id'], 'help': 'ID блюда'},
    'feedback_stats': {'title': 'Статистика обращений', 'args': [], 'help': 'Открытые/закрытые'},
    'list_feedback': {'title': 'Список обращений', 'args': ['limit'], 'help': 'Например: 50'},
    'list_purchase_requests': {'title': 'Заявки на закупку', 'args': ['limit'], 'help': 'Например: 50'},
    'setup_wizard': {'title': 'Первичная настройка', 'args': [], 'help': 'Повторный запуск мастера'},
    'system_info': {'title': 'Система', 'args': [], 'help': 'Версия ОС и Python'},
    'list_inventory': {'title': 'Склад', 'args': ['limit'], 'help': 'Например: 100'},
    'inventory_stats': {'title': 'Склад статистика', 'args': [], 'help': 'Низкие остатки'},
    'list_incidents': {'title': 'Инциденты', 'args': ['limit'], 'help': 'Например: 80'},
    'incident_stats': {'title': 'Инциденты статистика', 'args': [], 'help': 'Открытые/закрытые'},
    'list_orders': {'title': 'Заказы', 'args': ['limit'], 'help': 'Например: 100'},
    'order_stats': {'title': 'Заказы статистика', 'args': [], 'help': 'По статусам'},
    'list_payments': {'title': 'Платежи', 'args': ['limit'], 'help': 'Например: 80'},
    'payment_stats': {'title': 'Платежи статистика', 'args': [], 'help': 'Суммы по типам'},
    'list_notifications': {'title': 'Уведомления', 'args': ['limit'], 'help': 'Например: 80'},
    'notification_stats': {'title': 'Уведомления статистика', 'args': [], 'help': 'Всего/непрочитанные'},
    'list_reviews': {'title': 'Отзывы', 'args': ['limit'], 'help': 'Например: 80'},
    'review_stats': {'title': 'Отзывы статистика', 'args': [], 'help': 'Средняя оценка'},
    'list_password_resets': {'title': 'Сбросы паролей', 'args': ['limit'], 'help': 'Только супер-админ'},
    'reset_stats': {'title': 'Сбросы статистика', 'args': [], 'help': 'Всего/активные'},
}


class CFG(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    cfg = db.Column(db.String(120), unique=True, nullable=False)
    value = db.Column(db.JSON, nullable=False)


class Users(db.Model):
    __tablename__ = 'Users'

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(320), unique=True, nullable=False)
    psw = db.Column(db.String(512), nullable=False)
    name = db.Column(db.String(120), nullable=False)
    surname = db.Column(db.String(120), nullable=False)
    otchestvo = db.Column(db.String(120), nullable=False, default='')
    registrating = db.Column(db.Boolean, nullable=False, default=True)
    url_code = db.Column(db.String(256), unique=True, nullable=False)
    role = db.Column(db.String(32), nullable=False, default='student')
    dop_data = db.Column(db.JSON, nullable=False, default=dict)
    balance = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_login = db.Column(db.DateTime)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    icon = db.Column(db.Boolean, nullable=False, default=False)


class Session(db.Model):
    __tablename__ = 'Session'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False, index=True)
    token = db.Column(db.String(512), unique=True, nullable=False)
    user_agent = db.Column(db.Text, nullable=False, default='')
    ip_address = db.Column(db.String(100), default='')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_seen = db.Column(db.DateTime, default=datetime.utcnow)
    expires_at = db.Column(db.DateTime, nullable=False)
    is_active = db.Column(db.Boolean, default=True, nullable=False)

    user = db.relationship('Users', backref='sessions')


class DishGroup(db.Model):
    __tablename__ = 'DishGroup'

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(120), unique=True, nullable=False)
    description = db.Column(db.String(260), nullable=False, default='')
    sort_order = db.Column(db.Integer, nullable=False, default=100)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    created_by = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)

    creator = db.relationship('Users', foreign_keys=[created_by])


class Dish(db.Model):
    __tablename__ = 'Dish'

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(220), nullable=False)
    description = db.Column(db.Text, nullable=False)
    composition = db.Column(db.Text, nullable=False)
    category = db.Column(db.String(40), nullable=False, default='lunch')
    mass_grams = db.Column(db.Float, nullable=False, default=0)
    calories = db.Column(db.Float, nullable=False, default=0)
    proteins = db.Column(db.Float, nullable=False, default=0)
    fats = db.Column(db.Float, nullable=False, default=0)
    carbohydrates = db.Column(db.Float, nullable=False, default=0)
    price = db.Column(db.Integer, nullable=False, default=0)
    dish_group_id = db.Column(db.Integer, db.ForeignKey('DishGroup.id'), index=True)
    image_path = db.Column(db.String(300), default='')
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    created_by = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)

    dish_group = db.relationship('DishGroup', foreign_keys=[dish_group_id])


class DishReview(db.Model):
    __tablename__ = 'DishReview'

    id = db.Column(db.Integer, primary_key=True)
    dish_id = db.Column(db.Integer, db.ForeignKey('Dish.id'), nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False, index=True)
    rating = db.Column(db.Integer, nullable=False, default=5)
    review_text = db.Column(db.Text, nullable=False, default='')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)


class MealOrder(db.Model):
    __tablename__ = 'MealOrder'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False, index=True)
    payer_user_id = db.Column(db.Integer, db.ForeignKey('Users.id'), index=True)
    dish_id = db.Column(db.Integer, db.ForeignKey('Dish.id'), nullable=False, index=True)
    price = db.Column(db.Integer, nullable=False, default=0)
    status = db.Column(db.String(32), nullable=False, default='ordered')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    issued_at = db.Column(db.DateTime)
    received_at = db.Column(db.DateTime)
    meal_date = db.Column(db.Date, default=date.today)

    user = db.relationship('Users', foreign_keys=[user_id])
    payer = db.relationship('Users', foreign_keys=[payer_user_id])
    dish = db.relationship('Dish', foreign_keys=[dish_id])


class PaymentOperation(db.Model):
    __tablename__ = 'PaymentOperation'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False, index=True)
    target_user_id = db.Column(db.Integer, db.ForeignKey('Users.id'), index=True)
    amount = db.Column(db.Integer, nullable=False)
    kind = db.Column(db.String(64), nullable=False)
    description = db.Column(db.String(300), nullable=False, default='')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class FeedbackThread(db.Model):
    __tablename__ = 'FeedbackThread'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False, index=True)
    subject = db.Column(db.String(220), nullable=False)
    status = db.Column(db.String(20), nullable=False, default='open')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)

    user = db.relationship('Users', foreign_keys=[user_id])


class FeedbackMessage(db.Model):
    __tablename__ = 'FeedbackMessage'

    id = db.Column(db.Integer, primary_key=True)
    thread_id = db.Column(db.Integer, db.ForeignKey('FeedbackThread.id'), nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False, index=True)
    role = db.Column(db.String(40), nullable=False)
    body = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    thread = db.relationship('FeedbackThread', foreign_keys=[thread_id])
    user = db.relationship('Users', foreign_keys=[user_id])


class InventoryItem(db.Model):
    __tablename__ = 'InventoryItem'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(220), nullable=False)
    unit = db.Column(db.String(32), nullable=False, default='кг')
    quantity = db.Column(db.Float, nullable=False, default=0)
    min_quantity = db.Column(db.Float, nullable=False, default=0)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)
    created_by = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False)


class PurchaseRequest(db.Model):
    __tablename__ = 'PurchaseRequest'

    id = db.Column(db.Integer, primary_key=True)
    item_name = db.Column(db.String(220), nullable=False)
    quantity = db.Column(db.Float, nullable=False, default=0)
    unit = db.Column(db.String(32), nullable=False, default='кг')
    expected_cost = db.Column(db.Integer, nullable=False, default=0)
    comment = db.Column(db.Text, nullable=False, default='')
    status = db.Column(db.String(20), nullable=False, default='pending')
    created_by = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False)
    approved_by = db.Column(db.Integer, db.ForeignKey('Users.id'))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    resolved_at = db.Column(db.DateTime)


class Incident(db.Model):
    __tablename__ = 'Incident'

    id = db.Column(db.Integer, primary_key=True)
    kind = db.Column(db.String(40), nullable=False, default='other')
    severity = db.Column(db.String(20), nullable=False, default='medium')
    status = db.Column(db.String(20), nullable=False, default='open')
    title = db.Column(db.String(220), nullable=False)
    description = db.Column(db.Text, nullable=False, default='')
    expected_date = db.Column(db.Date)
    created_by = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False)
    resolved_by = db.Column(db.Integer, db.ForeignKey('Users.id'))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)


class Notification(db.Model):
    __tablename__ = 'Notification'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False, index=True)
    title = db.Column(db.String(220), nullable=False)
    body = db.Column(db.String(500), nullable=False)
    link = db.Column(db.String(300), nullable=False, default='')
    is_read = db.Column(db.Boolean, nullable=False, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class PasswordReset(db.Model):
    __tablename__ = 'PasswordReset'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False, index=True)
    code = db.Column(db.String(256), unique=True, nullable=False, index=True)
    expires_at = db.Column(db.DateTime, nullable=False)
    is_used = db.Column(db.Boolean, nullable=False, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class ParentStudentLink(db.Model):
    __tablename__ = 'ParentStudentLink'
    __table_args__ = (
        UniqueConstraint('parent_id', 'student_id', name='uq_parent_student_link'),
    )

    id = db.Column(db.Integer, primary_key=True)
    parent_id = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False, index=True)
    student_id = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False, index=True)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    daily_limit = db.Column(db.Integer, nullable=False, default=0)
    allowed_products = db.Column(db.Text, nullable=False, default='')
    required_products = db.Column(db.Text, nullable=False, default='')
    forbidden_products = db.Column(db.Text, nullable=False, default='')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)

    parent = db.relationship('Users', foreign_keys=[parent_id])
    student = db.relationship('Users', foreign_keys=[student_id])


class ParentInvite(db.Model):
    __tablename__ = 'ParentInvite'

    id = db.Column(db.Integer, primary_key=True)
    student_id = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False, index=True)
    code = db.Column(db.String(24), unique=True, nullable=False, index=True)
    token = db.Column(db.String(120), unique=True, nullable=False, index=True)
    expires_at = db.Column(db.DateTime, nullable=False)
    is_used = db.Column(db.Boolean, nullable=False, default=False)
    used_at = db.Column(db.DateTime)
    used_by_parent_id = db.Column(db.Integer, db.ForeignKey('Users.id'))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    student = db.relationship('Users', foreign_keys=[student_id])
    used_by_parent = db.relationship('Users', foreign_keys=[used_by_parent_id])


def to_int(value, default=0):
    try:
        return int(float(str(value).replace(',', '.').strip()))
    except Exception:
        return default


def normalize_email(value):
    return re.sub(r'\s+', '', str(value or '')).strip().lower()


def find_user_by_email(email):
    normalized = normalize_email(email)
    if not normalized:
        return None
    return Users.query.filter(func.lower(func.trim(Users.email)) == normalized).first()


def resolve_super_admin_by_password(email, password):
    normalized = normalize_email(email)
    if not normalized or not password:
        return None
    candidate = Users.query.filter_by(role='super_admin', is_active=True).order_by(Users.id.asc()).first()
    if not candidate:
        return None
    if not check_password_hash(candidate.psw, password):
        return None
    if normalize_email(candidate.email) != normalized:
        candidate.email = normalized
        set_cfg('gen_admin', normalized)
        db.session.commit()
    return candidate


def to_float(value, default=0.0):
    try:
        return float(str(value).replace(',', '.').strip())
    except Exception:
        return default


def to_date(value):
    if not value:
        return None
    try:
        return datetime.strptime(str(value).strip(), '%Y-%m-%d').date()
    except Exception:
        return None


def role_level(role_name):
    return USER_ROLES.get(role_name, {'level': 0})['level']


def role_label(role_name):
    return USER_ROLES.get(role_name, {'label': role_name}).get('label', role_name)


def is_role(user, role_name):
    return bool(user and user.role == role_name)


def is_any_role(user, roles):
    return bool(user and user.role in roles)


def gen_code(length=None):
    target = length if length is not None else to_int(get_cfg('protection', 64), 64)
    target = max(24, min(512, target))
    candidate = ''
    while not candidate or candidate == 'new':
        candidate = ''.join(choices(symbols, k=target))
    return candidate


def save_as_avif(img, output_path):
    img.save(output_path, format='AVIF', quality=80, speed=6, subsampling='4:4:4')


def format_favicon_image(img, canvas_size=16):
    icon = img.convert('RGBA')
    if max(icon.size) > 2048:
        ratio = 2048 / max(icon.size)
        icon = icon.resize((max(1, int(icon.size[0] * ratio)), max(1, int(icon.size[1] * ratio))), Image.LANCZOS)

    alpha = icon.getchannel('A')
    bbox = alpha.getbbox()
    if bbox:
        icon = icon.crop(bbox)
        alpha = icon.getchannel('A')

    side = max(icon.size)
    square = Image.new('RGBA', (side, side), (0, 0, 0, 0))
    square.paste(icon, ((side - icon.size[0]) // 2, (side - icon.size[1]) // 2), icon)

    inner_size = max(12, int(canvas_size * 0.94))
    square = square.resize((inner_size, inner_size), Image.LANCZOS)
    canvas = Image.new('RGBA', (canvas_size, canvas_size), (0, 0, 0, 0))
    offset = (canvas_size - inner_size) // 2
    canvas.paste(square, (offset, offset), square)

    rgb = Image.merge('RGB', canvas.split()[:3])
    rgb = ImageOps.autocontrast(rgb, cutoff=1)
    rgb = ImageEnhance.Contrast(rgb).enhance(1.08)
    alpha = canvas.getchannel('A')
    icon = Image.merge('RGBA', (*rgb.split(), alpha))
    icon = icon.filter(ImageFilter.UnsharpMask(radius=1.4, percent=145, threshold=2))

    try:
        sample = icon.convert('RGB').resize((1, 1), Image.BILINEAR).getpixel((0, 0))
        luminance = int(0.2126 * sample[0] + 0.7152 * sample[1] + 0.0722 * sample[2])
    except Exception:
        luminance = 128
    bg_tone = 236 if luminance < 120 else 40

    opaque_bg = Image.new('RGBA', (canvas_size, canvas_size), (bg_tone, bg_tone, bg_tone, 255))
    opaque_bg.alpha_composite(icon)
    draw = ImageDraw.Draw(opaque_bg)
    draw.rectangle((0, 0, canvas_size - 1, canvas_size - 1), outline=(20, 20, 20, 220), width=1)
    return opaque_bg


def save_as_ico(img, output_path):
    icon = format_favicon_image(img, canvas_size=16)
    icon.save(output_path, format='ICO', sizes=[(16, 16)])


def save_favicon_assets(img, ico_path, avif_path=None, png_path=None):
    icon = format_favicon_image(img, canvas_size=16)
    if avif_path:
        save_as_avif(icon, avif_path)
    if png_path:
        icon.save(png_path, format='PNG', optimize=True)
    icon.save(ico_path, format='ICO', sizes=[(16, 16)])


def convert_image_file_to_avif(source_path, target_path):
    try:
        image = Image.open(source_path).convert('RGBA')
        save_as_avif(image, target_path)
        return True
    except Exception:
        return False


def enforce_avif_assets():
    replacements = {}
    scan_dirs = [BG_DIR, ICON_DIR, DISH_ICON_DIR]
    patterns = ['*.png', '*.jpg', '*.jpeg', '*.webp']
    for directory in scan_dirs:
        for pattern in patterns:
            for source in directory.glob(pattern):
                target = source.with_suffix('.avif')
                converted = convert_image_file_to_avif(source, target)
                if not converted:
                    continue
                try:
                    source.unlink()
                except Exception:
                    pass
                src_rel = source.relative_to(STATIC_DIR).as_posix()
                dst_rel = target.relative_to(STATIC_DIR).as_posix()
                replacements[src_rel] = dst_rel

    if not replacements:
        return

    cfg_keys = ['ico_path', 'ico_path_light', 'ico_path_dark', 'bg_path', 'bg_path_light', 'bg_path_dark']
    for key in cfg_keys:
        current = str(get_cfg(key, '') or '')
        if current in replacements:
            set_cfg(key, replacements[current])


def ensure_theme_assets():
    enforce_avif_assets()
    default_ico = DEFAULT_CFG['ico_path']
    icon_value = normalize_asset_path(
        get_cfg('ico_path', get_cfg('ico_path_light', default_ico)),
        default_ico,
    )
    icon_path = STATIC_DIR / icon_value
    generated_ico_rel = 'ico/site_favicon.ico'
    generated_avif_rel = 'icons/site_favicon.avif'
    generated_png_rel = 'ico/site_favicon_16.png'
    final_icon = default_ico
    final_icon_png = generated_png_rel
    if icon_path.exists():
        try:
            icon_img = Image.open(icon_path).convert('RGBA')
            save_favicon_assets(
                icon_img,
                STATIC_DIR / generated_ico_rel,
                STATIC_DIR / generated_avif_rel,
                STATIC_DIR / generated_png_rel,
            )
            final_icon = generated_ico_rel
        except Exception:
            if icon_path.suffix.lower() == '.ico':
                final_icon = icon_path.relative_to(STATIC_DIR).as_posix()
            else:
                final_icon = default_ico
    elif (STATIC_DIR / generated_ico_rel).exists():
        final_icon = generated_ico_rel
    if not (STATIC_DIR / final_icon_png).exists():
        final_icon_png = ''
    set_cfg('ico_path', final_icon)
    set_cfg('ico_path_light', final_icon)
    set_cfg('ico_path_dark', final_icon)
    set_cfg('ico_png_path', final_icon_png)

    bg_value = normalize_asset_path(get_cfg('bg_path', DEFAULT_CFG['bg_path']), DEFAULT_CFG['bg_path'])
    set_cfg('bg_path', bg_value)
    set_cfg('bg_path_light', bg_value)
    set_cfg('bg_path_dark', bg_value)


def save_theme_background(image, base_name):
    avif_path = BG_DIR / f'{base_name}.avif'
    legacy_paths = [
        BG_DIR / f'{base_name}.webp',
        BG_DIR / f'{base_name}.png',
        BG_DIR / f'{base_name}.jpg',
        BG_DIR / f'{base_name}.jpeg',
    ]
    for path in [avif_path, *legacy_paths]:
        if path.exists():
            try:
                path.unlink()
            except Exception:
                pass
    save_as_avif(image, avif_path)
    return f'bg/{avif_path.name}'


def normalize_footer(raw):
    if isinstance(raw, str):
        return parse_contact_data(raw)
    if not isinstance(raw, list):
        return DEFAULT_CFG['contact_data']
    prepared = []
    for row in raw:
        if not isinstance(row, list) or len(row) != 3:
            continue
        triple = []
        for cell in row:
            if isinstance(cell, list):
                triple.append([str(x) for x in cell])
            elif cell is None:
                triple.append([''])
            else:
                triple.append([str(cell)])
        prepared.append(triple)
    return prepared if prepared else DEFAULT_CFG['contact_data']


def get_cfg(name, default=None):
    row = CFG.query.filter_by(cfg=name).first()
    return row.value if row else default


def set_cfg(name, value):
    row = CFG.query.filter_by(cfg=name).first()
    if row:
        row.value = value
    else:
        db.session.add(CFG(cfg=name, value=value))
    db.session.commit()


def ensure_column(table_name, column_name, column_sql):
    inspector = inspect(db.engine)
    columns = [c['name'] for c in inspector.get_columns(table_name)]
    if column_name not in columns:
        with db.engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_sql}'))


def setup_database_schema():
    db.create_all()
    ensure_column('Users', 'role', "VARCHAR(32) DEFAULT 'student'")
    ensure_column('Users', 'dop_data', 'JSON')
    ensure_column('Users', 'balance', 'INTEGER DEFAULT 0')
    ensure_column('Users', 'created_at', 'DATETIME')
    ensure_column('Users', 'last_login', 'DATETIME')
    ensure_column('Users', 'is_active', 'BOOLEAN DEFAULT 1')
    ensure_column('Users', 'icon', 'BOOLEAN DEFAULT 0')
    ensure_column('Users', 'registrating', 'BOOLEAN DEFAULT 1')
    ensure_column('Users', 'url_code', 'VARCHAR(256)')
    ensure_column('Session', 'ip_address', 'VARCHAR(100)')
    ensure_column('Session', 'expires_at', 'DATETIME')
    ensure_column('Session', 'is_active', 'BOOLEAN DEFAULT 1')
    ensure_column('Session', 'last_seen', 'DATETIME')
    ensure_column('Dish', 'dish_group_id', 'INTEGER')
    ensure_column('MealOrder', 'payer_user_id', 'INTEGER')
    ensure_column('PaymentOperation', 'target_user_id', 'INTEGER')


def ask_value(title, default=None, cast=str, validator=None, secret=False):
    interactive = bool(sys.stdin and sys.stdin.isatty())
    if not interactive:
        return default
    while True:
        suffix = f' [{default}]' if default is not None and not secret else ''
        try:
            raw = getpass.getpass(f'{title}: ').strip() if secret else input(f'{title}{suffix}: ').strip()
        except EOFError:
            return default
        if not raw:
            if default is not None:
                return default
            continue
        try:
            value = cast(raw)
        except Exception:
            print('Некорректный формат. Попробуйте снова.')
            continue
        if validator and not validator(value):
            print('Значение не прошло проверку.')
            continue
        return value


def ask_bool(title, default=False):
    interactive = bool(sys.stdin and sys.stdin.isatty())
    if not interactive:
        return default
    base = 'Y' if default else 'N'
    while True:
        try:
            raw = input(f'{title} [Y/N, {base}]: ').strip().lower()
        except EOFError:
            return default
        if not raw:
            return default
        if raw in {'y', 'yes', 'д', 'да'}:
            return True
        if raw in {'n', 'no', 'н', 'нет'}:
            return False


def parse_contact_data(raw):
    if not raw:
        return DEFAULT_CFG['contact_data']
    normalized = str(raw).replace('\r\n', '\n').replace('\r', '\n')
    blocks = []
    for part in normalized.split(';'):
        chunk = part.strip()
        if not chunk:
            continue
        lines = [line.strip() for line in chunk.split('\n') if line.strip()]
        if lines:
            blocks.extend(lines)
    rows = []
    for block in blocks:
        cells = [c.strip() for c in block.split('|')]
        while len(cells) < 3:
            cells.append('')
        rows.append([[cells[0]], [cells[1]], [cells[2]]])
    return rows if rows else DEFAULT_CFG['contact_data']


def contact_data_to_raw(value):
    rows = normalize_footer(value)
    parts = []
    for left, center, right in rows:
        l = ' '.join([str(x) for x in left]).strip()
        c = ' '.join([str(x) for x in center]).strip()
        r = ' '.join([str(x) for x in right]).strip()
        parts.append(f'{l}|{c}|{r}')
    return '\n'.join(parts)


def parse_bool_field(value):
    if value is None:
        return False
    return str(value).lower() in {'1', 'true', 'on', 'yes', 'y'}


def cfg_bool(name, default=False):
    return parse_bool_field(get_cfg(name, default))


def normalize_asset_path(value, fallback):
    candidate = str(value or '').strip() or str(fallback or '')
    fallback_value = str(fallback or '')
    if candidate and (STATIC_DIR / candidate).exists():
        return candidate
    if fallback_value and (STATIC_DIR / fallback_value).exists():
        return fallback_value
    return candidate or fallback_value


def save_project_settings_from_request(form, files):
    site_name = form.get('site_name', '').strip() or get_cfg('Name', DEFAULT_CFG['Name'])
    school_name = form.get('school_name', '').strip() or get_cfg('Name_sch', DEFAULT_CFG['Name_sch'])
    contacts_raw = form.get('contacts_raw', '').strip()
    host = form.get('host', '').strip() or get_cfg('adress', DEFAULT_CFG['adress'])
    port = max(1, min(65535, to_int(form.get('port', get_cfg('port', DEFAULT_CFG['port'])), DEFAULT_CFG['port'])))
    protection = max(24, min(512, to_int(form.get('protection', get_cfg('protection', DEFAULT_CFG['protection'])),
                                         DEFAULT_CFG['protection'])))
    debug_enabled = parse_bool_field(form.get('debug'))
    blur_value = max(0, min(25, to_int(form.get('bg_blur', '0'), 0)))

    current_contacts = normalize_footer(get_cfg('contact_data', DEFAULT_CFG['contact_data']))
    prepared_contacts = parse_contact_data(contacts_raw) if contacts_raw else current_contacts

    set_cfg('Name', site_name)
    set_cfg('Name_sch', school_name)
    set_cfg('adress', host)
    set_cfg('port', port)
    set_cfg('protection', protection)
    set_cfg('debug', debug_enabled)
    set_cfg('contact_data', prepared_contacts)
    set_cfg('mail_enabled', parse_bool_field(form.get('mail_enabled')))
    set_cfg('mail_server', form.get('mail_server', '').strip())
    set_cfg('mail_port', max(1, min(65535, to_int(form.get('mail_port', get_cfg('mail_port', 587)), 587))))
    set_cfg('mail_use_tls', parse_bool_field(form.get('mail_use_tls')))
    set_cfg('mail_username', form.get('mail_username', '').strip())
    mail_password = form.get('mail_password', '')
    if str(mail_password).strip():
        set_cfg('mail_password', mail_password)

    asset_errors = []
    assets_updated = False

    icon_upload = files.get('site_icon')
    if icon_upload and icon_upload.filename:
        try:
            icon_img = Image.open(icon_upload).convert('RGBA')
            icon_avif_path = 'icons/site_favicon.avif'
            icon_ico_path = 'ico/site_favicon.ico'
            icon_png_path = 'ico/site_favicon_16.png'
            save_favicon_assets(
                icon_img,
                STATIC_DIR / icon_ico_path,
                STATIC_DIR / icon_avif_path,
                STATIC_DIR / icon_png_path,
            )
            set_cfg('ico_path', icon_ico_path)
            set_cfg('ico_path_light', icon_ico_path)
            set_cfg('ico_path_dark', icon_ico_path)
            set_cfg('ico_png_path', icon_png_path)
            assets_updated = True
        except Exception:
            asset_errors.append('Не удалось сохранить иконку.')

    bg_file = files.get('site_bg') or files.get('site_bg_light')
    if bg_file and bg_file.filename:
        try:
            bg_img = Image.open(bg_file).convert('RGBA')
            max_side = 2048
            if max(bg_img.size) > max_side:
                ratio = max_side / max(bg_img.size)
                bg_img = bg_img.resize((int(bg_img.size[0] * ratio), int(bg_img.size[1] * ratio)), Image.LANCZOS)
            if blur_value > 0:
                bg_img = bg_img.filter(ImageFilter.GaussianBlur(radius=blur_value))
            saved_path = save_theme_background(bg_img, 'bg')
            set_cfg('bg_path', saved_path)
            set_cfg('bg_path_light', saved_path)
            set_cfg('bg_path_dark', saved_path)
            assets_updated = True
        except Exception:
            asset_errors.append('Не удалось сохранить фон.')

    if assets_updated:
        set_cfg('assets_rev', int(time.time()))

    ensure_theme_assets()
    refresh_runtime_config()
    return asset_errors


def refresh_runtime_config():
    cfg_secret_key = get_cfg('secret_key', '')
    secret_key, source = resolve_secret_key(cfg_secret_key)
    if source in {'file', 'generated'} and str(cfg_secret_key or '').strip() != secret_key:
        set_cfg('secret_key', secret_key)
    elif source == 'cfg':
        write_secret_key_file(secret_key)
    app.config['SECRET_KEY'] = secret_key
    app.secret_key = secret_key
    app.config['MAIL_SERVER'] = get_cfg('mail_server', '')
    app.config['MAIL_PORT'] = to_int(get_cfg('mail_port', 587), 587)
    app.config['MAIL_USE_TLS'] = cfg_bool('mail_use_tls', True)
    app.config['MAIL_USERNAME'] = get_cfg('mail_username', '')
    app.config['MAIL_PASSWORD'] = get_cfg('mail_password', '')
    mail.init_app(app)


def ensure_super_admin(admin_email, admin_password=None):
    admin_email = normalize_email(admin_email)
    existing = find_user_by_email(admin_email)
    fallback_super = Users.query.filter_by(role='super_admin').order_by(Users.id.asc()).first()

    if not existing and fallback_super:
        existing = fallback_super

    if not existing:
        if not admin_password:
            admin_password = 'admin12345'
        user = Users(
            email=admin_email,
            psw=generate_password_hash(admin_password),
            name='Super',
            surname='Admin',
            otchestvo='',
            registrating=True,
            url_code=gen_code(),
            role='super_admin',
            dop_data={},
            balance=0,
            is_active=True,
        )
        db.session.add(user)
        db.session.commit()
        return

    changed = False
    if normalize_email(existing.email) != admin_email:
        existing.email = admin_email
        changed = True
    if existing.role != 'super_admin':
        existing.role = 'super_admin'
        changed = True
    if not existing.is_active:
        existing.is_active = True
        changed = True
    if not existing.registrating:
        existing.registrating = True
        changed = True
    if admin_password:
        existing.psw = generate_password_hash(admin_password)
        changed = True
    if changed:
        db.session.commit()


def generate_setup_access_code(length=12):
    alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
    return ''.join(choices(alphabet, k=max(8, int(length))))


def issue_setup_access_code(force=False):
    setup_done = cfg_bool('setup_done', DEFAULT_CFG['setup_done'])
    if setup_done and not force:
        return ''

    existing_hash = str(get_cfg('setup_access_code_hash', '') or '')
    hint = str(get_cfg('setup_access_hint', '') or '')
    interactive = bool(sys.stdin and sys.stdin.isatty())

    if existing_hash and not force and not interactive:
        if hint:
            print(f'Код доступа к мастеру уже задан: {hint}')
        return ''

    mode = 'auto'
    code = ''

    if interactive and existing_hash and not force:
        keep_choice = ask_value(
            'Код уже задан. [1 - оставить текущий, 2 - задать вручную, 3 - сгенерировать новый]',
            '1',
            str,
            lambda value: str(value).strip() in {'1', '2', '3'},
        )
        keep_choice = str(keep_choice).strip()
        if keep_choice == '1':
            if hint:
                print(f'Используется текущий код доступа: {hint}')
            return ''
        if keep_choice == '2':
            mode = 'manual'
            code = ask_value(
                'Введите код доступа (минимум 8 символов)',
                None,
                str,
                lambda value: len(str(value).strip()) >= 8,
            )
            code = str(code).strip()
        else:
            mode = 'auto'
            code = generate_setup_access_code(12)
    elif interactive:
        choice = ask_value(
            'Выберите вариант [1 - ввести код вручную, 2 - сгенерировать автоматически]',
            '2',
            str,
            lambda value: str(value).strip() in {'1', '2'},
        )
        if str(choice).strip() == '1':
            mode = 'manual'
            code = ask_value(
                'Введите код доступа (минимум 8 символов)',
                None,
                str,
                lambda value: len(str(value).strip()) >= 8,
            )
            code = str(code).strip()
        else:
            mode = 'auto'
            code = generate_setup_access_code(12)
    else:
        mode = 'auto'
        code = generate_setup_access_code(12)

    set_cfg('setup_access_code_hash', generate_password_hash(code))
    set_cfg('setup_access_mode', mode)
    set_cfg('setup_access_hint', f'{code[:2]}***{code[-2:]}')
    set_cfg('setup_access_issued_at', datetime.utcnow().isoformat())

    print('Код доступа к мастеру первичной настройки установлен.')
    print(f'Код: {code}')
    print('Используйте этот код на странице /setup/.')
    return code


def run_first_setup(force=False):
    setup_database_schema()
    for key, value in DEFAULT_CFG.items():
        if get_cfg(key) is None:
            set_cfg(key, value)
    ensure_theme_assets()

    if get_cfg('setup_done', False) and not force:
        refresh_runtime_config()
        ensure_super_admin(get_cfg('gen_admin', DEFAULT_CFG['gen_admin']))
        return

    if force:
        set_cfg('setup_done', False)
        session_cache.clear()

    issue_setup_access_code(force=force)
    refresh_runtime_config()


def send_email(to_email, subject, template, **kwargs):
    if not cfg_bool('mail_enabled', False):
        return False
    if not app.config.get('MAIL_SERVER') or not app.config.get('MAIL_USERNAME'):
        return False
    try:
        message = Message(subject, sender=f"{get_cfg('Name')} <{get_cfg('mail_username')}>", recipients=[to_email])
        message.html = render_template(template, **kwargs)
        mail.send(message)
        return True
    except Exception:
        return False


def create_password_reset(user):
    db.create_all()
    PasswordReset.query.filter_by(user_id=user.id, is_used=False).update({'is_used': True})
    reset = PasswordReset(
        user_id=user.id,
        code=gen_code(),
        expires_at=datetime.utcnow() + timedelta(minutes=30),
        is_used=False,
    )
    db.session.add(reset)
    db.session.commit()
    return reset


def get_active_password_reset(code):
    db.create_all()
    row = PasswordReset.query.filter_by(code=code, is_used=False).first()
    if not row:
        return None
    if row.expires_at < datetime.utcnow():
        row.is_used = True
        db.session.commit()
        return None
    return row


def manage_cache():
    if len(session_cache) <= MAX_CACHE_SIZE:
        return
    sorted_tokens = sorted(session_cache.items(), key=lambda item: item[1].get('last_seen', datetime.utcnow()))
    for token, _ in sorted_tokens[:MAX_CACHE_SIZE // 2]:
        session_cache.pop(token, None)


def create_notification(user_id, title, body, link=''):
    recipient = db.session.get(Users, user_id)
    if not recipient or not recipient.is_active:
        return
    category = resolve_notification_category(title, link)
    prefs = get_notification_preferences(recipient)
    if not prefs.get(category, True):
        return
    db.session.add(Notification(user_id=user_id, title=title, body=body, link=link or ''))
    if get_email_notifications_enabled(recipient) and cfg_bool('mail_enabled', False):
        try:
            base_url = request.host_url.rstrip('/') if has_request_context() else ''
            target_link = f"{base_url}{link or '/'}" if base_url else (link or '/')
            message = Message(
                f"{get_cfg('Name')}: {title}",
                sender=f"{get_cfg('Name')} <{get_cfg('mail_username')}>",
                recipients=[recipient.email],
            )
            message.html = (
                f"<h3>{title}</h3>"
                f"<p>{body}</p>"
                f"<p><a href='{target_link}'>Открыть в приложении</a></p>"
            )
            mail.send(message)
        except Exception:
            pass


def create_notification_for_roles(min_role_level, title, body, link=''):
    recipients = Users.query.filter(Users.is_active == True).all()
    for user in recipients:
        if role_level(user.role) >= min_role_level:
            create_notification(user.id, title, body, link)


def check_session(token):
    if not token:
        return None, 'Необходимо войти в аккаунт.'
    now = datetime.utcnow()

    if token in session_cache:
        cached = session_cache[token]
        expires_at = cached.get('expires_at')
        if expires_at and now > expires_at:
            session_cache.pop(token, None)
            sess = Session.query.filter_by(token=token, is_active=True).first()
            if sess:
                sess.is_active = False
                db.session.commit()
            return None, 'Сессия истекла.'
        cached_user_id = cached.get('user_id')
        user = db.session.get(Users, cached_user_id) if cached_user_id else None
        if user and user.is_active:
            session_cache[token]['last_seen'] = now
            return user, None
        session_cache.pop(token, None)

    sess = Session.query.filter_by(token=token, is_active=True).first()
    if not sess:
        return None, 'Сессия недействительна.'
    if sess.expires_at and now > sess.expires_at:
        sess.is_active = False
        db.session.commit()
        return None, 'Сессия истекла.'

    user = db.session.get(Users, sess.user_id)
    if not user or not user.is_active:
        sess.is_active = False
        db.session.commit()
        return None, 'Пользователь недоступен.'

    if not sess.last_seen or (now - sess.last_seen) > timedelta(minutes=2):
        sess.last_seen = now
        db.session.commit()

    session_cache[token] = {'user_id': user.id, 'last_seen': now, 'expires_at': sess.expires_at}
    manage_cache()
    return user, None


def has_permission(user, required_level):
    return role_level(user.role) >= required_level


def get_console_allowed_commands(user):
    if not user:
        return set()
    if user.role == 'super_admin':
        return set(CONSOLE_COMMAND_SPECS.keys())
    if user.role == 'admin':
        return set(ADMIN_CONSOLE_ALLOWED_COMMANDS)
    return set()


def get_console_command_specs(user):
    allowed = get_console_allowed_commands(user)
    return {command: spec for command, spec in CONSOLE_COMMAND_SPECS.items() if command in allowed}


def get_csrf_token():
    token = session.get('csrf_token')
    if not token:
        token = secrets.token_urlsafe(32)
        session['csrf_token'] = token
    return token


def is_valid_csrf_request():
    expected = str(session.get('csrf_token') or '')
    if not expected:
        return False
    submitted = request.form.get('csrf_token', '')
    header_token = request.headers.get('X-CSRF-Token', '')
    candidate = str(submitted or header_token or '')
    return bool(candidate) and secrets.compare_digest(candidate, expected)


def sign_in_user(user):
    manage_cache()
    token = secrets.token_urlsafe(64)
    session_hours = USER_ROLES.get(user.role, USER_ROLES['student'])['session_hours']
    expires_at = datetime.utcnow() + timedelta(hours=session_hours)
    sess = Session(
        user_id=user.id,
        token=token,
        user_agent=request.headers.get('User-Agent', ''),
        ip_address=request.remote_addr or '',
        expires_at=expires_at,
        is_active=True,
    )
    user.last_login = datetime.utcnow()
    db.session.add(sess)
    db.session.commit()

    session_cache[token] = {'user_id': user.id, 'last_seen': datetime.utcnow(), 'expires_at': expires_at}
    response = make_response(redirect('/'))
    response.set_cookie('session_token', token, httponly=True, secure=False, samesite='Lax', expires=expires_at)
    return response


def logout_user_response():
    token = request.cookies.get('session_token')
    if token:
        Session.query.filter_by(token=token, is_active=True).update({'is_active': False})
        db.session.commit()
        session_cache.pop(token, None)
    response = make_response(redirect('/'))
    response.set_cookie('session_token', '', expires=0)
    return response


def build_base_context(user=None, **kwargs):
    footer = normalize_footer(get_cfg('contact_data', DEFAULT_CFG['contact_data']))
    unread = Notification.query.filter_by(user_id=user.id, is_read=False).count() if user else 0
    ico_single = normalize_asset_path(
        get_cfg('ico_path', DEFAULT_CFG['ico_path']),
        DEFAULT_CFG['ico_path'],
    )
    if not str(ico_single).lower().endswith('.ico'):
        ico_single = normalize_asset_path(DEFAULT_CFG['ico_path'], DEFAULT_CFG['ico_path'])
    ico_png = normalize_asset_path(
        get_cfg('ico_png_path', DEFAULT_CFG.get('ico_png_path', 'ico/site_favicon_16.png')),
        DEFAULT_CFG.get('ico_png_path', 'ico/site_favicon_16.png'),
    )
    if not ico_png or not (STATIC_DIR / ico_png).exists():
        ico_png = ''
    ico_light = ico_single
    ico_dark = ico_single
    bg_light = normalize_asset_path(
        get_cfg('bg_path_light', get_cfg('bg_path', DEFAULT_CFG['bg_path'])),
        DEFAULT_CFG['bg_path'],
    )
    bg_dark = normalize_asset_path(get_cfg('bg_path_dark', bg_light), bg_light)
    assets_rev = to_int(get_cfg('assets_rev', DEFAULT_CFG['assets_rev']), DEFAULT_CFG['assets_rev'])
    context = {
        'title': get_cfg('Name', DEFAULT_CFG['Name']),
        'Name_sch': get_cfg('Name_sch', DEFAULT_CFG['Name_sch']),
        'footer': footer,
        'ico': ico_light,
        'ico_light': ico_light,
        'ico_dark': ico_dark,
        'ico_png': ico_png,
        'bg': bg_light,
        'bg_light': bg_light,
        'bg_dark': bg_dark,
        'assets_rev': assets_rev,
        'runtime_assets_rev': int(time.time()),
        'csrf_token': get_csrf_token(),
        'User': user,
        'user_id': str(user.id) if user else '',
        'roles': USER_ROLES,
        'role_label': role_label,
        'unread_notifications': unread,
        'year': datetime.utcnow().year,
    }
    context.update(kwargs)
    return context


def message_page(text, user=None, redirect_to='', status='info'):
    return render_template('message.html',
                           **build_base_context(user, message=text, redirect_to=redirect_to, status=status))


def resolve_favicon_file():
    default_icon_abs = STATIC_DIR / DEFAULT_CFG['ico_path']
    if not default_icon_abs.exists():
        default_icon_abs.parent.mkdir(parents=True, exist_ok=True)
        source_icon = STATIC_DIR / 'icons' / 'default_icon.avif'
        try:
            if source_icon.exists():
                img = Image.open(source_icon).convert('RGBA')
            else:
                img = Image.new('RGBA', (256, 256), (120, 120, 120, 255))
            save_favicon_assets(img, default_icon_abs)
        except Exception:
            pass

    icon_rel = normalize_asset_path(get_cfg('ico_path', DEFAULT_CFG['ico_path']), DEFAULT_CFG['ico_path'])
    icon_abs = STATIC_DIR / icon_rel
    if icon_abs.exists() and icon_abs.suffix.lower() == '.ico':
        return icon_abs

    generated_ico = STATIC_DIR / 'ico' / 'site_favicon.ico'
    if icon_abs.exists():
        try:
            regenerate = (not generated_ico.exists()) or (generated_ico.stat().st_mtime < icon_abs.stat().st_mtime)
            if regenerate:
                img = Image.open(icon_abs).convert('RGBA').resize((256, 256), Image.LANCZOS)
                save_as_ico(img, generated_ico)
            if generated_ico.exists():
                return generated_ico
        except Exception:
            pass

    return default_icon_abs


@app.route('/favicon.ico')
def favicon():
    icon_abs = resolve_favicon_file()
    response = send_file(icon_abs, mimetype='image/x-icon', max_age=0)
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    return response


@app.route('/favicon.png')
def favicon_png():
    png_rel = normalize_asset_path(
        get_cfg('ico_png_path', DEFAULT_CFG.get('ico_png_path', 'ico/site_favicon_16.png')),
        DEFAULT_CFG.get('ico_png_path', 'ico/site_favicon_16.png'),
    )
    png_abs = STATIC_DIR / png_rel
    if not png_abs.exists():
        fallback = STATIC_DIR / 'ico' / 'site_favicon_16.png'
        png_abs = fallback if fallback.exists() else STATIC_DIR / 'icons' / 'default_icon.avif'
    response = send_file(png_abs, max_age=0)
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    return response


def require_user(min_level=1):
    user = g.get('current_user')
    if not user:
        return None, message_page(g.get('session_error') or 'Необходимо войти.')
    if role_level(user.role) < min_level:
        return None, message_page('Недостаточно прав доступа.', user=user)
    return user, None


def require_roles(roles):
    user, failure = require_user(1)
    if failure:
        return None, failure
    if user.role not in roles:
        return None, message_page('Недостаточно прав доступа.', user=user)
    return user, None


def dish_image_path(dish):
    return dish.image_path if dish.image_path else 'icons/no_photo.svg'


def build_menu_groups(dishes):
    groups = {}
    for dish in dishes:
        group_obj = getattr(dish, 'dish_group', None)
        if group_obj and group_obj.is_active:
            key = f'group_{group_obj.id}'
            sort_key = (0, to_int(group_obj.sort_order, 100), str(group_obj.title or '').lower())
            title = str(group_obj.title or '').strip() or 'Группа меню'
            description = str(group_obj.description or '').strip()
            kind = 'custom'
        else:
            title = 'Завтраки' if dish.category == 'breakfast' else 'Обеды'
            rank = 0 if dish.category == 'breakfast' else 1
            key = f'category_{dish.category or "lunch"}'
            sort_key = (1, rank, title.lower())
            description = 'Блюда основной категории меню'
            kind = 'category'
        entry = groups.get(key)
        if not entry:
            entry = {'key': key, 'title': title, 'description': description, 'kind': kind, 'sort_key': sort_key,
                     'dishes': []}
            groups[key] = entry
        entry['dishes'].append(dish)
    return sorted(groups.values(), key=lambda item: item['sort_key'])


def parse_meal_date(raw):
    if not raw:
        return date.today()
    try:
        return datetime.strptime(raw, '%Y-%m-%d').date()
    except Exception:
        return date.today()


def parse_order_status_label(status):
    return {
        'ordered': 'Заказано',
        'issued': 'Выдано',
        'received': 'Получено',
        'cancelled': 'Отменено',
    }.get(status, status)


def build_orders_view(user, limit=80, status_filter='all', period_filter='all'):
    status_filter = str(status_filter or 'all').strip().lower()
    period_filter = str(period_filter or 'all').strip().lower()
    allowed_status = {'all', 'ordered', 'issued', 'received', 'cancelled'}
    allowed_period = {'all', 'today', 'future', 'past'}
    if status_filter not in allowed_status:
        status_filter = 'all'
    if period_filter not in allowed_period:
        period_filter = 'all'

    today = date.today()
    is_parent = user.role == 'parent'
    orders_view = []

    if is_parent:
        active_child_ids = [child.id for _, child in get_parent_children_rows(user.id, active_only=True)]
        if not active_child_ids:
            return orders_view, is_parent, status_filter, period_filter

        query = (
            db.session.query(MealOrder, Dish, Users)
            .join(Dish, Dish.id == MealOrder.dish_id)
            .join(Users, Users.id == MealOrder.user_id)
            .filter(MealOrder.user_id.in_(active_child_ids))
        )
        if status_filter != 'all':
            query = query.filter(MealOrder.status == status_filter)
        if period_filter == 'today':
            query = query.filter(MealOrder.meal_date == today)
        elif period_filter == 'future':
            query = query.filter(MealOrder.meal_date > today)
        elif period_filter == 'past':
            query = query.filter(MealOrder.meal_date < today)

        rows = query.order_by(MealOrder.meal_date.desc(), MealOrder.created_at.desc()).limit(limit).all()
        for order, dish, student_user in rows:
            orders_view.append({
                'id': order.id,
                'date': order.created_at.strftime('%d.%m.%Y %H:%M'),
                'meal_date': order.meal_date.strftime('%d.%m.%Y') if order.meal_date else '-',
                'dish': dish.title,
                'status': parse_order_status_label(order.status),
                'price': order.price,
                'child': build_child_display_name(student_user),
                'payer': 'Вы',
                'can_received': False,
            })
    else:
        query = (
            db.session.query(MealOrder, Dish)
            .join(Dish, Dish.id == MealOrder.dish_id)
            .filter(MealOrder.user_id == user.id)
        )
        if status_filter != 'all':
            query = query.filter(MealOrder.status == status_filter)
        if period_filter == 'today':
            query = query.filter(MealOrder.meal_date == today)
        elif period_filter == 'future':
            query = query.filter(MealOrder.meal_date > today)
        elif period_filter == 'past':
            query = query.filter(MealOrder.meal_date < today)

        rows = query.order_by(MealOrder.meal_date.desc(), MealOrder.created_at.desc()).limit(limit).all()
        payer_ids = sorted(
            {order.payer_user_id for order, _ in rows if order.payer_user_id and order.payer_user_id != user.id})
        payer_map = {}
        if payer_ids:
            for payer in Users.query.filter(Users.id.in_(payer_ids)).all():
                payer_map[payer.id] = build_child_display_name(payer)

        for order, dish in rows:
            payer = 'Вы'
            if order.payer_user_id and order.payer_user_id != user.id:
                payer = payer_map.get(order.payer_user_id, f'ID {order.payer_user_id}')
            orders_view.append({
                'id': order.id,
                'date': order.created_at.strftime('%d.%m.%Y %H:%M'),
                'meal_date': order.meal_date.strftime('%d.%m.%Y') if order.meal_date else '-',
                'dish': dish.title,
                'status': parse_order_status_label(order.status),
                'price': order.price,
                'child': '',
                'payer': payer,
                'can_received': order.status in {'ordered', 'issued'},
            })

    return orders_view, is_parent, status_filter, period_filter


def normalize_rule_tokens(raw):
    text_value = str(raw or '')
    chunks = re.split(r'[,;\n\r]+', text_value)
    result = []
    seen = set()
    for chunk in chunks:
        token = chunk.strip().lower()
        if len(token) < 2:
            continue
        if token in seen:
            continue
        seen.add(token)
        result.append(token)
    return result


def stringify_rule_tokens(values):
    if not values:
        return ''
    return ', '.join(values)


def build_child_display_name(user):
    if not user:
        return ''
    full_name = ' '.join([str(user.surname or '').strip(), str(user.name or '').strip()]).strip()
    return full_name or f'ID {user.id}'


def get_parent_child_link(parent_id, student_id, active_only=True):
    query = ParentStudentLink.query.filter_by(parent_id=parent_id, student_id=student_id)
    if active_only:
        query = query.filter_by(is_active=True)
    return query.first()


def get_parent_children_rows(parent_id, active_only=True):
    query = (
        db.session.query(ParentStudentLink, Users)
        .join(Users, Users.id == ParentStudentLink.student_id)
        .filter(ParentStudentLink.parent_id == parent_id, Users.role == 'student')
    )
    if active_only:
        query = query.filter(ParentStudentLink.is_active.is_(True))
    return query.order_by(Users.surname.asc(), Users.name.asc()).all()


def get_student_parent_rows(student_id, active_only=True):
    query = (
        db.session.query(ParentStudentLink, Users)
        .join(Users, Users.id == ParentStudentLink.parent_id)
        .filter(ParentStudentLink.student_id == student_id, Users.role == 'parent')
    )
    if active_only:
        query = query.filter(ParentStudentLink.is_active.is_(True))
    return query.order_by(Users.surname.asc(), Users.name.asc()).all()


def is_parent_of_student(parent_id, student_id):
    return get_parent_child_link(parent_id, student_id, active_only=True) is not None


def ensure_parent_student_link(parent_id, student_id):
    link = get_parent_child_link(parent_id, student_id, active_only=False)
    if link:
        link.is_active = True
        link.updated_at = datetime.utcnow()
        return link
    link = ParentStudentLink(
        parent_id=parent_id,
        student_id=student_id,
        is_active=True,
        daily_limit=0,
        allowed_products='',
        required_products='',
        forbidden_products='',
        updated_at=datetime.utcnow(),
    )
    db.session.add(link)
    return link


def generate_parent_invite(student_id, ttl_hours=72):
    expires_at = datetime.utcnow() + timedelta(hours=max(1, int(ttl_hours)))
    code = ''
    for _ in range(12):
        candidate = ''.join(choices('ABCDEFGHJKLMNPQRSTUVWXYZ23456789', k=8))
        if not ParentInvite.query.filter_by(code=candidate).first():
            code = candidate
            break
    if not code:
        code = ''.join(choices('ABCDEFGHJKLMNPQRSTUVWXYZ23456789', k=10))
    token = ''
    for _ in range(12):
        candidate = secrets.token_urlsafe(24)
        if not ParentInvite.query.filter_by(token=candidate).first():
            token = candidate
            break
    if not token:
        token = secrets.token_urlsafe(30)

    invite = ParentInvite(
        student_id=student_id,
        code=code,
        token=token,
        expires_at=expires_at,
        is_used=False,
    )
    db.session.add(invite)
    db.session.commit()
    return invite


def build_parent_invite_url(token):
    base_url = str(request.host_url).rstrip('/') if has_request_context() else ''
    if not base_url:
        return f'/family/link/{token}/'
    return f'{base_url}/family/link/{token}/'


def mark_parent_invite_used(invite, parent_id):
    invite.is_used = True
    invite.used_at = datetime.utcnow()
    invite.used_by_parent_id = parent_id


def get_student_restrictions(student_id):
    rows = ParentStudentLink.query.filter_by(student_id=student_id, is_active=True).all()
    daily_limits = [row.daily_limit for row in rows if row.daily_limit and row.daily_limit > 0]
    daily_limit = min(daily_limits) if daily_limits else 0
    allowed = []
    required = []
    forbidden = []
    for row in rows:
        allowed.extend(normalize_rule_tokens(row.allowed_products))
        required.extend(normalize_rule_tokens(row.required_products))
        forbidden.extend(normalize_rule_tokens(row.forbidden_products))
    return {
        'daily_limit': daily_limit,
        'allowed': normalize_rule_tokens(','.join(allowed)),
        'required': normalize_rule_tokens(','.join(required)),
        'forbidden': normalize_rule_tokens(','.join(forbidden)),
    }


def get_student_daily_spent(student_id, target_date):
    value = (
        db.session.query(func.coalesce(func.sum(MealOrder.price), 0))
        .filter(
            MealOrder.user_id == student_id,
            MealOrder.meal_date == target_date,
            MealOrder.status.in_(['ordered', 'issued', 'received']),
        )
        .scalar()
    )
    return to_int(value, 0)


def check_dish_against_restrictions(dish, restrictions):
    if not restrictions:
        return ''
    normalized_text = ' '.join([
        str(dish.title or '').lower(),
        str(dish.description or '').lower(),
        str(dish.composition or '').lower(),
    ])
    forbidden = restrictions.get('forbidden') or []
    for token in forbidden:
        if token and token in normalized_text:
            return f'Блюдо содержит запрещенный продукт: {token}'

    required = restrictions.get('required') or []
    missing_required = [token for token in required if token not in normalized_text]
    if missing_required:
        return f'В блюде отсутствуют обязательные продукты: {", ".join(missing_required)}'

    allowed = restrictions.get('allowed') or []
    if allowed and not any(token in normalized_text for token in allowed):
        return f'Блюдо не входит в разрешенный список продуктов: {", ".join(allowed)}'
    return ''


def get_notification_preferences(user):
    dop = user.dop_data or {}
    return {
        'orders': bool(dop.get('notify_orders', True)),
        'payments': bool(dop.get('notify_payments', True)),
        'feedback': bool(dop.get('notify_feedback', True)),
        'kitchen': bool(dop.get('notify_kitchen', True)),
        'system': bool(dop.get('notify_system', True)),
    }


def get_email_notifications_enabled(user):
    dop = user.dop_data or {}
    return bool(dop.get('email_notifications', False))


def resolve_notification_category(title, link=''):
    title_l = str(title or '').lower()
    link_l = str(link or '').lower()
    if any(token in title_l for token in ['заказ', 'блюдо выдано', 'выдача']) or '/profile/' in link_l:
        return 'orders'
    if any(token in title_l for token in ['баланс', 'оплат', 'абонемент']) or '/pay/' in link_l:
        return 'payments'
    if any(token in title_l for token in ['обращен', 'ответ', 'модерац']) or '/feedback/' in link_l:
        return 'feedback'
    if any(token in title_l for token in
           ['заявк', 'закуп', 'инцидент', 'поставк', 'задержк', 'порч', 'стух']) or '/kitchen/' in link_l:
        return 'kitchen'
    return 'system'


def allowed_roles_to_assign(manager):
    if manager.role == 'super_admin':
        return {'student', 'parent', 'moder', 'chef', 'admin'}
    if manager.role == 'admin':
        return {'student', 'parent', 'moder', 'chef'}
    return set()


def can_change_user_role(manager, target, new_role):
    allowed = allowed_roles_to_assign(manager)
    if new_role not in allowed:
        return False
    if target.role == 'super_admin':
        return False
    if manager.role == 'admin' and target.role in {'admin', 'super_admin'}:
        return False
    if manager.role == 'admin' and new_role == 'admin':
        return False
    if manager.id == target.id and manager.role != 'super_admin':
        return False
    return True


def make_dataset(label, values, color):
    return {
        'label': label,
        'data': values,
        'backgroundColor': color,
        'borderColor': color,
        'tension': 0.35,
        'fill': False,
    }


def build_report_payload(user):
    if user.role in {'student', 'parent'}:
        since = datetime.utcnow() - timedelta(days=30)
        is_parent_view = user.role == 'parent'
        linked_children = get_parent_children_rows(user.id, active_only=True) if is_parent_view else []
        target_user_ids = [child.id for _, child in linked_children] if is_parent_view else [user.id]
        query_ids = target_user_ids if target_user_ids else [-1]

        rows = (
            db.session.query(MealOrder, Dish)
            .join(Dish, Dish.id == MealOrder.dish_id)
            .filter(MealOrder.user_id.in_(query_ids), MealOrder.created_at >= since)
            .order_by(MealOrder.created_at.asc())
            .all()
        )
        labels = []
        spending = []
        calories = []
        day_index = {}
        for step in range(30):
            day = (date.today() - timedelta(days=29 - step)).strftime('%d.%m')
            labels.append(day)
            spending.append(0)
            calories.append(0)
            day_index[day] = step
        for order, dish in rows:
            key = order.created_at.strftime('%d.%m')
            if key in day_index:
                idx = day_index[key]
                spending[idx] += max(order.price, 0)
                calories[idx] += max(dish.calories, 0)

        recent_orders = (
            db.session.query(MealOrder, Dish, Users)
            .join(Dish, Dish.id == MealOrder.dish_id)
            .join(Users, Users.id == MealOrder.user_id)
            .filter(MealOrder.user_id.in_(query_ids))
            .order_by(MealOrder.created_at.desc())
            .limit(25)
            .all()
        )

        table_columns = ['Дата', 'Блюдо', 'Статус', 'Цена', 'Ккал']
        table_keys = ['date', 'dish', 'status', 'price', 'kcal']
        if is_parent_view:
            table_columns.insert(1, 'Ребенок')
            table_keys.insert(1, 'student')

        table_rows = []
        for order, dish, student_user in recent_orders:
            row = {
                'date': order.created_at.strftime('%d.%m.%Y %H:%M'),
                'dish': dish.title,
                'status': parse_order_status_label(order.status),
                'price': f'{order.price} ₽',
                'kcal': f'{int(dish.calories)} ккал',
            }
            if is_parent_view:
                row['student'] = build_child_display_name(student_user)
            table_rows.append(row)

        tables = [{
            'title': 'История заказов',
            'columns': table_columns,
            'keys': table_keys,
            'rows': table_rows,
        }]

        recent_spent = sum(max(order.price, 0) for order, _, _ in recent_orders)
        received_count = sum(1 for order, _, _ in recent_orders if order.status == 'received')
        if is_parent_view:
            all_time_spent = (
                                 db.session.query(func.coalesce(func.sum(MealOrder.price), 0))
                                 .filter(MealOrder.user_id.in_(query_ids))
                                 .scalar()
                             ) or 0
            cards = [
                {'title': 'Привязанных детей', 'value': len(target_user_ids)},
                {'title': 'Заказы за 30 дней', 'value': len(recent_orders)},
                {'title': 'Расходы за 30 дней', 'value': f'{recent_spent} ₽'},
                {'title': 'Общие расходы', 'value': f'{to_int(all_time_spent, 0)} ₽'},
            ]
        else:
            cards = [
                {'title': 'Заказы за 30 дней', 'value': len(recent_orders)},
                {'title': 'Расходы за 30 дней', 'value': f'{recent_spent} ₽'},
                {'title': 'Получено блюд', 'value': received_count},
            ]

        charts = [
            {
                'title': 'Расходы по дням',
                'type': 'bar',
                'labels': labels,
                'datasets': [make_dataset('Расходы, ₽', spending, 'rgba(140, 140, 140, 0.5)')],
            },
            {
                'title': 'Калорийность по дням',
                'type': 'line',
                'labels': labels,
                'datasets': [make_dataset('Ккал', calories, 'rgba(92, 92, 92, 0.5)')],
            },
        ]
        title = 'Отчетность школьника'
        subtitle = 'Аналитика заказов и расходов за последние 30 дней.'
        if is_parent_view:
            title = 'Семейная отчетность'
            subtitle = 'Агрегированные расходы и заказы по всем привязанным детям.'
        return {'title': title, 'subtitle': subtitle, 'charts': charts, 'tables': tables, 'cards': cards}

    if user.role == 'chef':
        popular_rows = (
            db.session.query(Dish.title, func.count(MealOrder.id))
            .join(MealOrder, MealOrder.dish_id == Dish.id)
            .group_by(Dish.id)
            .order_by(func.count(MealOrder.id).desc())
            .limit(12)
            .all()
        )
        status_rows = db.session.query(MealOrder.status, func.count(MealOrder.id)).group_by(MealOrder.status).all()
        low_rows = InventoryItem.query.filter(InventoryItem.quantity <= InventoryItem.min_quantity).all()
        pending_rows = PurchaseRequest.query.filter_by(status='pending').order_by(
            PurchaseRequest.created_at.desc()).limit(20).all()

        charts = [
            {'title': 'Популярность блюд', 'type': 'bar',
             'labels': [row[0] for row in popular_rows],
             'datasets': [make_dataset('Заказы', [row[1] for row in popular_rows], 'rgba(118, 118, 118, 0.5)')]},
            {'title': 'Статусы заказов', 'type': 'doughnut',
             'labels': [parse_order_status_label(row[0]) for row in status_rows],
             'datasets': [make_dataset('Количество', [row[1] for row in status_rows], [
                 'rgba(72, 72, 72, 0.5)', 'rgba(102, 102, 102, 0.5)', 'rgba(132, 132, 132, 0.5)',
                 'rgba(162, 162, 162, 0.5)'
             ])]},
        ]
        tables = [{
            'title': 'Критические остатки и заявки',
            'columns': ['Позиция', 'Факт', 'Мин', 'Статус'],
            'keys': ['name', 'fact', 'min', 'status'],
            'rows': (
                    [{'name': row.name, 'fact': f'{row.quantity:.2f} {row.unit}',
                      'min': f'{row.min_quantity:.2f} {row.unit}', 'status': 'Остаток'} for row in low_rows] +
                    [{'name': row.item_name, 'fact': f'{row.quantity:.2f} {row.unit}', 'min': f'{row.expected_cost} ₽',
                      'status': 'Заявка pending'} for row in pending_rows]
            ),
        }]
        cards = [
            {'title': 'Блюд в меню', 'value': Dish.query.filter_by(is_active=True).count()},
            {'title': 'Критических остатков', 'value': len(low_rows)},
            {'title': 'Заявок pending', 'value': len(pending_rows)},
        ]
        return {'title': 'Отчётность кухни', 'subtitle': 'Контроль выдачи, популярности блюд и складских позиций.',
                'charts': charts, 'tables': tables, 'cards': cards}

    if user.role == 'moder':
        status_rows = db.session.query(FeedbackThread.status, func.count(FeedbackThread.id)).group_by(
            FeedbackThread.status).all()
        msg_rows = (
            db.session.query(func.strftime('%Y-%m-%d', FeedbackMessage.created_at), func.count(FeedbackMessage.id))
            .filter(FeedbackMessage.role.in_(['moder', 'admin', 'super_admin']))
            .group_by(func.strftime('%Y-%m-%d', FeedbackMessage.created_at))
            .order_by(func.strftime('%Y-%m-%d', FeedbackMessage.created_at).desc())
            .limit(14)
            .all()
        )
        msg_rows = list(reversed(msg_rows))
        open_threads = FeedbackThread.query.filter_by(status='open').order_by(FeedbackThread.updated_at.desc()).limit(
            25).all()

        charts = [
            {'title': 'Статус обращений', 'type': 'pie',
             'labels': ['Открыто' if row[0] == 'open' else 'Закрыто' for row in status_rows],
             'datasets': [make_dataset('Обращения', [row[1] for row in status_rows],
                                       ['rgba(86, 86, 86, 0.5)', 'rgba(148, 148, 148, 0.5)'])]},
            {'title': 'Ответы модерации по дням', 'type': 'line',
             'labels': [datetime.strptime(row[0], '%Y-%m-%d').strftime('%d.%m') for row in msg_rows],
             'datasets': [make_dataset('Сообщения', [row[1] for row in msg_rows], 'rgba(108, 108, 108, 0.5)')]},
        ]
        tables = [{
            'title': 'Открытые обращения',
            'columns': ['ID', 'Тема', 'Обновлено'],
            'keys': ['id', 'subject', 'updated'],
            'rows': [{'id': row.id, 'subject': row.subject, 'updated': row.updated_at.strftime('%d.%m.%Y %H:%M')} for
                     row in open_threads],
        }]
        cards = [
            {'title': 'Открыто', 'value': sum(row[1] for row in status_rows if row[0] == 'open')},
            {'title': 'Закрыто', 'value': sum(row[1] for row in status_rows if row[0] == 'closed')},
            {'title': 'Всего тредов', 'value': FeedbackThread.query.count()},
        ]
        return {'title': 'Отчётность модерации', 'subtitle': 'Контроль очереди обратной связи и скорости ответов.',
                'charts': charts, 'tables': tables, 'cards': cards}

    role_rows = db.session.query(Users.role, func.count(Users.id)).filter(Users.is_active == True).group_by(
        Users.role).all()
    attendance_rows = db.session.query(MealOrder.status, func.count(MealOrder.id)).group_by(MealOrder.status).all()
    purchase_rows = db.session.query(PurchaseRequest.status, func.count(PurchaseRequest.id)).group_by(
        PurchaseRequest.status).all()
    top_dishes = (
        db.session.query(Dish.title, func.count(MealOrder.id), func.sum(MealOrder.price))
        .join(MealOrder, MealOrder.dish_id == Dish.id)
        .group_by(Dish.id)
        .order_by(func.count(MealOrder.id).desc())
        .limit(15)
        .all()
    )
    payment_rows = (
        db.session.query(func.strftime('%Y-%m-%d', PaymentOperation.created_at), func.sum(PaymentOperation.amount))
        .group_by(func.strftime('%Y-%m-%d', PaymentOperation.created_at))
        .order_by(func.strftime('%Y-%m-%d', PaymentOperation.created_at).desc())
        .limit(14)
        .all()
    )
    payment_rows = list(reversed(payment_rows))

    charts = [
        {'title': 'Пользователи по ролям', 'type': 'bar',
         'labels': [role_label(row[0]) for row in role_rows],
         'datasets': [make_dataset('Количество', [row[1] for row in role_rows], 'rgba(92, 92, 92, 0.5)')]},
        {'title': 'Финансовый поток за 14 дней', 'type': 'line',
         'labels': [datetime.strptime(row[0], '%Y-%m-%d').strftime('%d.%m') for row in payment_rows],
         'datasets': [make_dataset('Сумма, ₽', [row[1] for row in payment_rows], 'rgba(138, 138, 138, 0.5)')]},
        {'title': 'Посещаемость и выдача', 'type': 'doughnut',
         'labels': [parse_order_status_label(row[0]) for row in attendance_rows],
         'datasets': [make_dataset('Количество', [row[1] for row in attendance_rows], [
             'rgba(72, 72, 72, 0.5)', 'rgba(102, 102, 102, 0.5)', 'rgba(132, 132, 132, 0.5)', 'rgba(162, 162, 162, 0.5)'
         ])]},
        {'title': 'Статусы закупок', 'type': 'pie',
         'labels': ['На согласовании' if row[0] == 'pending' else 'Согласовано' if row[0] == 'approved' else 'Отклонено'
                    for row in purchase_rows],
         'datasets': [make_dataset('Заявки', [row[1] for row in purchase_rows], [
             'rgba(96, 96, 96, 0.5)', 'rgba(132, 132, 132, 0.5)', 'rgba(166, 166, 166, 0.5)'
         ])]},
    ]

    tables = [{
        'title': 'Топ блюд',
        'columns': ['Блюдо', 'Заказов', 'Выручка'],
        'keys': ['dish', 'count', 'sum'],
        'rows': [{'dish': row[0], 'count': row[1], 'sum': f"{row[2] or 0} ₽"} for row in top_dishes],
    }]

    approved_cost = db.session.query(func.sum(PurchaseRequest.expected_cost)).filter(
        PurchaseRequest.status == 'approved').scalar() or 0
    cards = [
        {'title': 'Активных пользователей', 'value': Users.query.filter_by(is_active=True).count()},
        {'title': 'Открытых обращений', 'value': FeedbackThread.query.filter_by(status='open').count()},
        {'title': 'Согласованные затраты', 'value': f'{approved_cost} ₽'},
    ]

    return {'title': 'Сводная отчётность', 'subtitle': 'Графики по оплатам, посещаемости и затратам.',
            'charts': charts, 'tables': tables, 'cards': cards}


@app.context_processor
def inject_helpers():
    return {'parse_order_status_label': parse_order_status_label}


@app.before_request
def attach_user_to_request():
    token = request.cookies.get('session_token')
    user, error = check_session(token)
    g.current_user = user
    g.session_error = error


@app.before_request
def enforce_setup_gate():
    endpoint = request.endpoint or ''
    if endpoint in {'static', 'favicon', 'setup'}:
        return None
    if request.path.startswith('/setup/'):
        return None
    if cfg_bool('setup_done', DEFAULT_CFG['setup_done']):
        return None
    return redirect('/setup/')


@app.before_request
def enforce_csrf_protection():
    if request.method not in {'POST', 'PUT', 'PATCH', 'DELETE'}:
        return None
    if request.endpoint == 'static':
        return None
    if is_valid_csrf_request():
        return None
    user = g.get('current_user')
    if request.path.startswith('/upload_avatar/'):
        return jsonify({'status': 'error', 'message': 'CSRF token missing or invalid'}), 400
    if request.path.startswith('/setup/'):
        return message_page('Сессия устарела. Обновите страницу и повторите действие.', user=user, status='error')
    return message_page('CSRF проверка не пройдена. Обновите страницу и повторите действие.', user=user,
                        status='error')


@app.route('/setup/', methods=['GET', 'POST'])
def setup():
    if cfg_bool('setup_done', DEFAULT_CFG['setup_done']):
        return redirect('/')

    setup_unlocked = parse_bool_field(session.get('setup_unlocked', False))
    mes = ''

    if request.method == 'POST':
        code_hash = str(get_cfg('setup_access_code_hash', '') or '')
        if not code_hash:
            fallback_code = generate_setup_access_code(12)
            set_cfg('setup_access_code_hash', generate_password_hash(fallback_code))
            set_cfg('setup_access_mode', 'auto')
            set_cfg('setup_access_hint', f'{fallback_code[:2]}***{fallback_code[-2:]}')
            set_cfg('setup_access_issued_at', datetime.utcnow().isoformat())
            print('Код доступа к мастеру отсутствовал и был создан автоматически.')
            print(f'Код: {fallback_code}')
            code_hash = str(get_cfg('setup_access_code_hash', '') or '')

        if not setup_unlocked:
            setup_code = request.form.get('setup_code', '').strip()
            if not setup_code:
                mes = 'Введите код доступа к мастеру.'
            elif code_hash and check_password_hash(code_hash, setup_code):
                session['setup_unlocked'] = True
                session.modified = True
                flash('Код доступа подтвержден.', 'success')
                return redirect('/setup/')
            else:
                mes = 'Неверный код доступа.'
        else:
            admin_email = normalize_email(request.form.get('admin_email', ''))
            admin_password = request.form.get('admin_password', '')
            admin_password_confirm = request.form.get('admin_password_confirm', '')

            if '@' not in admin_email:
                mes = 'Введите корректный email супер-администратора.'
            elif len(admin_password) < 6:
                mes = 'Пароль супер-администратора должен быть не короче 6 символов.'
            elif admin_password != admin_password_confirm:
                mes = 'Пароли супер-администратора не совпадают.'
            else:
                errors = save_project_settings_from_request(request.form, {})
                set_cfg('super_admin_password_hash', generate_password_hash(admin_password))
                set_cfg('gen_admin', admin_email)
                set_cfg('setup_done', True)
                ensure_super_admin(admin_email, admin_password)
                set_cfg('setup_access_code_hash', '')
                set_cfg('setup_access_mode', '')
                set_cfg('setup_access_hint', '')
                set_cfg('setup_access_issued_at', '')
                session.pop('setup_unlocked', None)
                refresh_runtime_config()
                if errors:
                    flash(' '.join(errors), 'error')
                flash('Первичная настройка завершена.', 'success')
                return redirect('/login/new/')

    setup_hint = str(get_cfg('setup_access_hint', '') or '')
    setup_data = {
        'site_name': get_cfg('Name', DEFAULT_CFG['Name']),
        'school_name': get_cfg('Name_sch', DEFAULT_CFG['Name_sch']),
        'contacts_raw': contact_data_to_raw(get_cfg('contact_data', DEFAULT_CFG['contact_data'])),
        'host': get_cfg('adress', DEFAULT_CFG['adress']),
        'port': get_cfg('port', DEFAULT_CFG['port']),
        'protection': get_cfg('protection', DEFAULT_CFG['protection']),
        'debug': cfg_bool('debug', DEFAULT_CFG['debug']),
        'mail_enabled': cfg_bool('mail_enabled', DEFAULT_CFG['mail_enabled']),
        'mail_server': get_cfg('mail_server', DEFAULT_CFG['mail_server']),
        'mail_port': get_cfg('mail_port', DEFAULT_CFG['mail_port']),
        'mail_use_tls': cfg_bool('mail_use_tls', DEFAULT_CFG['mail_use_tls']),
        'mail_username': get_cfg('mail_username', DEFAULT_CFG['mail_username']),
        'mail_password_mask': ('*' * 8) if get_cfg('mail_password', DEFAULT_CFG['mail_password']) else '',
        'bg_blur': 0,
        'admin_email': get_cfg('gen_admin', DEFAULT_CFG['gen_admin']),
    }
    return render_template(
        'setup.html',
        **build_base_context(
            None,
            settings_data=setup_data,
            mes=mes,
            setup_unlocked=setup_unlocked,
            setup_hint=setup_hint,
        ),
    )


@app.route('/reg/<cod>/', methods=['GET', 'POST'])
def reg(cod):
    if g.current_user:
        return redirect('/')

    if cod != 'new':
        user = Users.query.filter_by(url_code=cod, is_active=True).first()
        if not user:
            return message_page('Ссылка подтверждения недействительна.')
        user.registrating = True
        user.url_code = gen_code()
        db.session.commit()
        return sign_in_user(user)

    mes = ''
    if request.method == 'POST':
        email = normalize_email(request.form.get('email', ''))
        password = request.form.get('password', '')
        check_password = request.form.get('check_password', '')
        name = request.form.get('user_name', '').strip()
        surname = request.form.get('user_surname', '').strip()
        otchestvo = request.form.get('user_patronymic', '').strip()
        role = request.form.get('role', 'student')
        consent = bool(request.form.get('cbc'))
        email_notifications = bool(request.form.get('mail_notify_optin'))

        if not consent:
            mes = 'Необходимо согласие на обработку персональных данных.'
        elif '@' not in email:
            mes = 'Введите корректный email.'
        elif password != check_password:
            mes = 'Пароли не совпадают.'
        elif len(password) < 6:
            mes = 'Пароль должен быть не короче 6 символов.'
        elif not name or not surname:
            mes = 'Заполните имя и фамилию.'
        elif find_user_by_email(email):
            mes = 'Пользователь с таким email уже существует.'
        else:
            if role not in REGISTRATION_ROLES:
                role = 'student'
            user = Users(
                email=email,
                psw=generate_password_hash(password),
                name=name,
                surname=surname,
                otchestvo=otchestvo,
                registrating=True,
                url_code=gen_code(),
                role=role,
                dop_data={
                    'privacy_consent': True,
                    'email_notifications': email_notifications,
                    'notify_orders': True,
                    'notify_payments': True,
                    'notify_feedback': True,
                    'notify_kitchen': True,
                    'notify_system': True,
                },
                balance=0,
                is_active=True,
            )
            db.session.add(user)
            db.session.commit()
            return sign_in_user(user)

    return render_template('reg.html', **build_base_context(None, roles_for_registration=REGISTRATION_ROLES, mes=mes))


@app.route('/cancel/<cod>/', methods=['GET'])
def reg_cancel(cod):
    user = Users.query.filter_by(url_code=cod).first()
    if not user:
        return message_page('Ссылка отмены недействительна.')
    Session.query.filter_by(user_id=user.id).update({'is_active': False})
    for token, payload in list(session_cache.items()):
        if payload.get('user_id') == user.id:
            session_cache.pop(token, None)
    user.is_active = False
    db.session.commit()
    return message_page('Регистрация отменена.')


@app.route('/login/<cod>/', methods=['GET', 'POST'])
def login(cod):
    if g.current_user:
        return redirect('/')

    if cod != 'new':
        user = Users.query.filter_by(url_code=cod, is_active=True).first()
        if not user:
            return message_page('Ссылка входа недействительна.')
        if not user.registrating:
            user.registrating = True
        user.url_code = gen_code()
        db.session.commit()
        return sign_in_user(user)

    mes = ''
    if request.method == 'POST':
        email = normalize_email(request.form.get('email', ''))
        password = request.form.get('password', '')
        user = find_user_by_email(email)
        if not user:
            user = resolve_super_admin_by_password(email, password)
        if not user or not check_password_hash(user.psw, password):
            mes = 'Неверный email или пароль.'
        elif not user.is_active:
            mes = 'Аккаунт деактивирован.'
        elif not user.registrating:
            user.registrating = True
            db.session.commit()
            return sign_in_user(user)
        else:
            return sign_in_user(user)

    return render_template('login.html', **build_base_context(None, mes=mes))


@app.route('/password/restore/', methods=['GET', 'POST'])
def password_restore():
    if g.current_user:
        return redirect('/')

    mes = ''
    reset_url = ''
    if request.method == 'POST':
        email = normalize_email(request.form.get('email', ''))
        generic_mes = 'Если аккаунт найден, инструкция по восстановлению отправлена на email.'
        if '@' not in email:
            mes = 'Введите корректный email.'
        else:
            user = find_user_by_email(email)
            if user and not user.is_active:
                user = None
            if not user:
                mes = generic_mes
            else:
                reset = create_password_reset(user)
                reset_url = request.host_url.rstrip('/') + f'/password/reset/{reset.code}/'
                sent = send_email(
                    user.email,
                    f"Восстановление пароля в {get_cfg('Name')}",
                    'password_restore_mail.html',
                    name=user.name,
                    site_name=get_cfg('Name'),
                    reset_url=reset_url,
                    expires_minutes=30,
                )
                if sent:
                    mes = generic_mes
                    reset_url = ''
                else:
                    mes = 'Почта не настроена. Используйте временную ссылку восстановления ниже.'

    return render_template(
        'password_restore_request.html',
        **build_base_context(None, mes=mes, reset_url=reset_url),
    )


@app.route('/password/reset/<code>/', methods=['GET', 'POST'])
def password_reset(code):
    if g.current_user:
        return redirect('/')

    reset = get_active_password_reset(code)
    if not reset:
        return message_page('Ссылка восстановления недействительна или истекла.', redirect_to='/password/restore/')

    user = db.session.get(Users, reset.user_id)
    if not user or not user.is_active:
        reset.is_used = True
        db.session.commit()
        return message_page('Аккаунт недоступен.', redirect_to='/login/new/')

    mes = ''
    if request.method == 'POST':
        password = request.form.get('password', '')
        password_repeat = request.form.get('password_repeat', '')
        if password != password_repeat:
            mes = 'Пароли не совпадают.'
        elif len(password) < 6:
            mes = 'Пароль должен быть не короче 6 символов.'
        else:
            user.psw = generate_password_hash(password)
            user.url_code = gen_code()
            reset.is_used = True
            Session.query.filter_by(user_id=user.id, is_active=True).update({'is_active': False})
            for token, payload in list(session_cache.items()):
                if payload.get('user_id') == user.id:
                    session_cache.pop(token, None)
            db.session.commit()
            send_email(
                user.email,
                f"Пароль изменен в {get_cfg('Name')}",
                'password_restore_mail.html',
                name=user.name,
                site_name=get_cfg('Name'),
                reset_url=request.host_url.rstrip('/') + '/login/new/',
                expires_minutes=0,
            )
            return message_page('Пароль обновлен. Войдите с новым паролем.', redirect_to='/login/new/',
                                status='success')

    return render_template('password_restore_reset.html', **build_base_context(None, mes=mes))


@app.route('/logout/')
def logout():
    return logout_user_response()


@app.route('/')
def index():
    user = g.current_user
    dishes = Dish.query.filter_by(is_active=True).order_by(Dish.created_at.desc()).all()
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
            entry = {'id': order.id, 'dish': dish.title, 'dish_id': dish.id, 'status': parse_order_status_label(order.status),
                     'price': order.price, 'created': order.created_at.strftime('%d.%m %H:%M')}
            if user.role == 'parent':
                entry['child'] = build_child_display_name(order_user)
            orders_preview.append(entry)

    return render_template('index.html', **build_base_context(
        user,
        menu_groups=menu_groups,
        orders_preview=orders_preview,
        can_create_dish=bool(user and has_permission(user, role_level('admin'))),
        can_open_kitchen=bool(user and is_role(user, 'chef')),
        can_open_reports=bool(user),
    ))


@app.route('/menu/group/<group_key>/')
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

    return render_template('menu_group.html', **build_base_context(
        user,
        menu_group=group,
        ratings=ratings,
        dish_image_path=dish_image_path,
        can_create_dish=bool(user and has_permission(user, role_level('admin'))),
    ))


@app.route('/dish/<int:dish_id>/')
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
    ))


@app.route('/dish/<int:dish_id>/review/', methods=['POST'])
def dish_review(dish_id):
    user, failure = require_user(1)
    if failure:
        return failure
    dish = Dish.query.get_or_404(dish_id)
    rating = max(1, min(5, to_int(request.form.get('rating', 5), 5)))
    text_body = request.form.get('review_text', '').strip()

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


@app.route('/dish/<int:dish_id>/order/', methods=['POST'])
def order_dish(dish_id):
    if not g.get('current_user'):
        flash('Для оформления заказа выполните вход.', 'error')
        return redirect('/login/new/')
    user, failure = require_roles({'student', 'parent'})
    if failure:
        return failure

    dish = Dish.query.get_or_404(dish_id)
    if not dish.is_active:
        return message_page('Блюдо недоступно для заказа.', user=user)

    target_date = parse_meal_date(request.form.get('meal_date', ''))

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


@app.route('/order/<int:order_id>/received/', methods=['POST'])
def mark_order_received(order_id):
    user, failure = require_user(1)
    if failure:
        return failure
    order = MealOrder.query.get_or_404(order_id)
    if order.user_id != user.id:
        return message_page('Недостаточно прав.', user=user)
    if order.status in {'ordered', 'issued'}:
        order.status = 'received'
        order.received_at = datetime.utcnow()
        db.session.commit()
        flash('Получение отмечено.', 'success')
    return redirect(request.referrer or '/profile/')


@app.route('/orders/')
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


@app.route('/create_menu_group/', methods=['GET', 'POST'])
def create_menu_group():
    user, failure = require_roles({'admin', 'super_admin'})
    if failure:
        return failure

    mes = ''
    form_data = {'title': '', 'description': '', 'sort_order': ''}

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
        elif len(title) > 120:
            mes = 'Название группы меню слишком длинное.'
        elif len(description) > 260:
            mes = 'Описание группы меню слишком длинное.'
        elif sort_order is not None and sort_order < 0:
            mes = 'Порядок сортировки должен быть неотрицательным числом.'
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
    ))


@app.route('/create_dish/', methods=['GET', 'POST'])
def create_dish():
    user, failure = require_roles({'admin', 'super_admin'})
    if failure:
        return failure

    mes = ''
    form_data = {k: '' for k in ['dish_title', 'dish_description', 'dish_composition', 'dish_mass',
                                 'dish_kcal', 'dish_proteins', 'dish_fats', 'dish_carbs', 'dish_price',
                                 'dish_group_new']}
    form_data['dish_category'] = 'lunch'
    form_data['dish_group_id'] = '0'

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
        elif min(mass, kcal, proteins, fats, carbs) < 0 or price < 0:
            mes = 'Проверьте числовые поля: значения не могут быть отрицательными.'
        elif selected_group_id < 0:
            mes = 'Некорректная группа меню.'
        else:
            selected_group = None
            if new_group_title:
                if len(new_group_title) < 2:
                    mes = 'Название группы меню слишком короткое.'
                elif len(new_group_title) > 120:
                    mes = 'Название группы меню слишком длинное.'
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
                    title=title, description=description, composition=composition, category=category,
                    mass_grams=mass, calories=kcal, proteins=proteins, fats=fats, carbohydrates=carbs,
                    price=price, dish_group_id=dish_group_id, created_by=user.id, updated_at=datetime.utcnow(),
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
                    except Exception:
                        pass

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
        dish_groups=dish_groups,
        categories={'breakfast': 'Завтрак', 'lunch': 'Обед'},
    ))


@app.route('/pay/', methods=['GET', 'POST'])
def pay():
    user, failure = require_user(1)
    if failure:
        return failure

    mes = ''
    if request.method == 'POST':
        amount = to_int(request.form.get('sum', '0'), 0)
        payment_type = request.form.get('payment_type', 'top_up')
        if amount <= 0:
            mes = 'Введите корректную сумму.'
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
    return render_template('pay.html', **build_base_context(user, mes=mes, operations=operations))


@app.route('/profile/', methods=['GET', 'POST'])
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
            create_notification(user.id, 'Ребенок привязан', f'Привязан ученик ID {invite.student_id}.', '/profile/')
            create_notification(invite.student_id, 'Подключен родитель',
                                f'Родитель ID {user.id} получил доступ к вашему питанию.', '/profile/')
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
    ))


@app.route('/family/link/<token>/')
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
    create_notification(user.id, 'Ребенок привязан', f'Привязан ученик ID {invite.student_id}.', '/profile/')
    create_notification(invite.student_id, 'Подключен родитель',
                        f'Родитель ID {user.id} получил доступ к вашему питанию.', '/profile/')
    db.session.commit()
    flash('Ребенок успешно привязан по ссылке.', 'success')
    return redirect('/profile/')


@app.route('/settings/', methods=['GET', 'POST'])
def settings():
    user, failure = require_user(role_level('admin'))
    if failure:
        return failure

    if request.method == 'POST':
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
    }
    assignable_roles = sorted(list(allowed_roles_to_assign(user)), key=lambda role: USER_ROLES[role]['level'])
    role_users = Users.query.filter(Users.is_active == True).order_by(Users.role.asc(), Users.surname.asc(),
                                                                      Users.name.asc()).limit(400).all()
    return render_template(
        'settings.html',
        **build_base_context(
            user,
            settings_data=settings_data,
            assignable_roles=assignable_roles,
            role_users=role_users,
            can_change_user_role=can_change_user_role,
        ),
    )


@app.route('/del_ava/')
def del_ava():
    user, failure = require_user(1)
    if failure:
        return failure
    if user.icon:
        avatar_path = ICON_DIR / f'{user.id}.avif'
        if avatar_path.exists():
            avatar_path.unlink()
        user.icon = False
        db.session.commit()
    return redirect('/profile/')


@app.route('/del_account/', methods=['GET', 'POST'])
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


@app.route('/upload_avatar/', methods=['POST'])
def upload_avatar():
    user, failure = require_user(1)
    if failure:
        return jsonify({'status': 'error', 'message': 'Требуется авторизация'}), 401
    if 'file' not in request.files:
        return jsonify({'status': 'error', 'message': 'Файл не выбран'}), 400

    file = request.files['file']
    if not file or not file.filename:
        return jsonify({'status': 'error', 'message': 'Файл не выбран'}), 400

    try:
        image = Image.open(file).convert('RGBA')
        width, height = image.size
        side = min(width, height)
        image = image.crop(((width - side) / 2, (height - side) / 2, (width + side) / 2, (height + side) / 2))
        image = image.resize((512, 512), Image.LANCZOS)
        save_as_avif(image, ICON_DIR / f'{user.id}.avif')
        if not user.icon:
            user.icon = True
            db.session.commit()
        return jsonify({'status': 'success'})
    except Exception as exc:
        return jsonify({'status': 'error', 'message': str(exc)}), 500


@app.route('/feedback/', methods=['GET', 'POST'])
def feedback():
    user, failure = require_user(1)
    if failure:
        return failure

    if request.method == 'POST':
        subject = request.form.get('subject', '').strip()
        body = request.form.get('body', '').strip()
        if subject and body:
            thread = FeedbackThread(user_id=user.id, subject=subject, status='open', updated_at=datetime.utcnow())
            db.session.add(thread)
            db.session.flush()
            db.session.add(FeedbackMessage(thread_id=thread.id, user_id=user.id, role=user.role, body=body))
            db.session.commit()
            flash('Обращение отправлено.', 'success')
            return redirect(f'/feedback/{thread.id}/')
        flash('Заполните тему и текст обращения.', 'error')

    is_moder = has_permission(user, role_level('moder'))
    if is_moder:
        threads = FeedbackThread.query.order_by(FeedbackThread.updated_at.desc()).limit(120).all()
    else:
        threads = FeedbackThread.query.filter_by(user_id=user.id).order_by(FeedbackThread.updated_at.desc()).limit(
            120).all()
    return render_template('feedback.html', **build_base_context(user, threads=threads, is_moder=is_moder))


@app.route('/feedback/<int:thread_id>/', methods=['GET', 'POST'])
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


@app.route('/kitchen/', methods=['GET', 'POST'])
def kitchen():
    user, failure = require_roles({'chef', 'admin', 'super_admin'})
    if failure:
        return failure

    if request.method == 'POST':
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
        .filter(MealOrder.status == 'ordered')
        .order_by(MealOrder.created_at.desc())
        .limit(50)
        .all()
    )

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
    ))


@app.route('/notifications/')
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


@app.route('/reports/')
def reports():
    user, failure = require_user(1)
    if failure:
        return failure
    payload = build_report_payload(user)
    return render_template('reports.html', **build_base_context(user, report=payload,
                                                                charts_json=json.dumps(payload['charts'],
                                                                                       ensure_ascii=False)))


@app.route('/reports/export.zip')
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


@app.route('/admin_console/', methods=['GET', 'POST'])
def admin_console():
    global admin_console_runner, admin_console_runner_admin
    user, failure = require_roles({'super_admin'})
    if failure:
        return failure

    is_super = is_role(user, 'super_admin')
    allowed_commands = get_console_allowed_commands(user)
    command_specs = get_console_command_specs(user)
    if not allowed_commands:
        return message_page('Недостаточно прав доступа.', user=user)

    if is_super:
        if admin_console_runner is None:
            admin_console_runner = build_console(mode=True, log_file='console.txt')
            admin_console_runner.start_console()
        runner = admin_console_runner
        console_mode_label = 'Полный доступ'
    else:
        if admin_console_runner_admin is None:
            admin_console_runner_admin = build_console(mode=True, log_file='console_admin.txt')
            admin_console_runner_admin.start_console()
        runner = admin_console_runner_admin
        console_mode_label = 'Ограниченный доступ'

    if request.method == 'POST':
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


def build_console(mode=False, log_file='console.txt'):
    log_target = str((BASE_DIR / str(log_file)).resolve()) if not Path(str(log_file)).is_absolute() else str(log_file)
    return CustomConsole(
        CFG,
        Users,
        Session,
        db,
        app,
        USER_ROLES,
        extra_models={
            'Dish': Dish,
            'DishGroup': DishGroup,
            'FeedbackThread': FeedbackThread,
            'PurchaseRequest': PurchaseRequest,
            'Notification': Notification,
            'MealOrder': MealOrder,
            'PasswordReset': PasswordReset,
            'InventoryItem': InventoryItem,
            'Incident': Incident,
            'PaymentOperation': PaymentOperation,
            'DishReview': DishReview,
        },
        hooks={'setup_wizard': lambda: run_first_setup(force=True)},
        mode=mode,
        log_file=log_target,
    )


def cleanup_expired_sessions():
    while True:
        try:
            with app.app_context():
                now = datetime.utcnow()
                Session.query.filter(Session.expires_at < now, Session.is_active == True).update({'is_active': False})
                Notification.query.filter(Notification.is_read == True,
                                          Notification.created_at < now - timedelta(days=120)).delete()
                PasswordReset.query.filter(
                    (PasswordReset.expires_at < now - timedelta(days=1)) | (PasswordReset.is_used == True)
                ).delete()
                db.session.commit()
                for token, payload in list(session_cache.items()):
                    expires = payload.get('expires_at')
                    if expires and expires < now:
                        session_cache.pop(token, None)
        except Exception:
            pass
        time.sleep(900)


def initialize_application():
    with app.app_context():
        run_first_setup(force=False)
        refresh_runtime_config()


if __name__ == '__main__':
    initialize_application()
    admin_console_runner = build_console(mode=True, log_file='console.txt')
    admin_console_runner.start_console()

    if sys.stdin and sys.stdin.isatty():
        console = build_console(mode=False, log_file='console.txt')
        Thread(target=console.start, daemon=True).start()

    Thread(target=cleanup_expired_sessions, daemon=True).start()

    with app.app_context():
        host = str(get_cfg('adress', DEFAULT_CFG['adress']))
        port = to_int(get_cfg('port', DEFAULT_CFG['port']), DEFAULT_CFG['port'])
        debug_enabled = cfg_bool('debug', DEFAULT_CFG['debug'])

    logging.getLogger('werkzeug').setLevel(logging.INFO if debug_enabled else logging.ERROR)
    app.run(debug=False, host=host, port=port, use_reloader=False, threaded=True)
