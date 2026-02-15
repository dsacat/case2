import getpass
import html as html_module
import io
import json
import logging
import os
import re
import secrets
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from random import choices
from threading import Lock, Thread

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
    'announcement': '',
    'announcement_type': 'info',
    'low_balance_threshold': 100,
    'topup_max_amount': 10000,
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
_last_cleanup_time = None
MAX_CACHE_SIZE = 2000
admin_console_runner = None
admin_console_runner_admin = None

_rl_lock = Lock()
rate_limit_store = {}
LOGIN_MAX_ATTEMPTS = 5
LOGIN_BLOCK_MINUTES = 15


def check_rate_limit(ip):
    with _rl_lock:
        entry = rate_limit_store.get(ip)
        if not entry:
            return None, 0
        blocked_until = entry.get('blocked_until')
        if blocked_until and datetime.utcnow() < blocked_until:
            remaining = max(1, int((blocked_until - datetime.utcnow()).total_seconds() // 60) + 1)
            return blocked_until, remaining
        return None, 0


def record_failed_attempt(ip):
    with _rl_lock:
        entry = rate_limit_store.setdefault(ip, {'count': 0, 'blocked_until': None})
        entry['count'] += 1
        if entry['count'] >= LOGIN_MAX_ATTEMPTS:
            entry['blocked_until'] = datetime.utcnow() + timedelta(minutes=LOGIN_BLOCK_MINUTES)
            entry['count'] = 0


def clear_rate_limit(ip):
    with _rl_lock:
        rate_limit_store.pop(ip, None)

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
    email = db.Column(db.String(320), unique=True, nullable=False, index=True)
    psw = db.Column(db.String(512), nullable=False)
    name = db.Column(db.String(120), nullable=False)
    surname = db.Column(db.String(120), nullable=False)
    otchestvo = db.Column(db.String(120), nullable=False, default='')
    registrating = db.Column(db.Boolean, nullable=False, default=True)
    url_code = db.Column(db.String(256), unique=True, nullable=False)
    role = db.Column(db.String(32), nullable=False, default='student', index=True)
    dop_data = db.Column(db.JSON, nullable=False, default=dict)
    balance = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_login = db.Column(db.DateTime)
    is_active = db.Column(db.Boolean, default=True, nullable=False, index=True)
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
    allergens = db.Column(db.Text, nullable=False, default='')
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


class WeeklyMenu(db.Model):
    __tablename__ = 'WeeklyMenu'
    __table_args__ = (db.UniqueConstraint('dish_id', 'day_of_week', name='uq_weekly_menu_dish_day'),)

    id = db.Column(db.Integer, primary_key=True)
    dish_id = db.Column(db.Integer, db.ForeignKey('Dish.id'), nullable=False, index=True)
    day_of_week = db.Column(db.Integer, nullable=False, index=True)
    created_by = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    dish = db.relationship('Dish', foreign_keys=[dish_id])


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
    pre_order_date = db.Column(db.Date, nullable=True, index=True)

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


class EmailVerification(db.Model):
    __tablename__ = 'EmailVerification'

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(320), nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=True, index=True)
    token = db.Column(db.String(256), unique=True, nullable=False, index=True)
    expires_at = db.Column(db.DateTime, nullable=False)
    is_verified = db.Column(db.Boolean, nullable=False, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class LoginOTP(db.Model):
    __tablename__ = 'LoginOTP'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False, index=True)
    code = db.Column(db.String(6), nullable=False)
    ip_address = db.Column(db.String(100), nullable=False, default='')
    attempts = db.Column(db.Integer, nullable=False, default=0)
    max_attempts = db.Column(db.Integer, nullable=False, default=3)
    expires_at = db.Column(db.DateTime, nullable=False)
    is_used = db.Column(db.Boolean, nullable=False, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class PendingPasswordChange(db.Model):
    __tablename__ = 'PendingPasswordChange'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('Users.id'), nullable=False, index=True)
    new_password_hash = db.Column(db.String(512), nullable=False)
    token = db.Column(db.String(256), unique=True, nullable=False, index=True)
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
    limits = db.Column(db.Text, nullable=False, default='{}')
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


def is_valid_email(email):
    email = normalize_email(email)
    if not email or len(email) < 5 or len(email) > 254:
        return False
    email_pattern = r'^[a-zA-Z0-9.!#$%&\'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$'
    return re.match(email_pattern, email) is not None


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
    except Exception as e:
        app.logger.debug(f'Could not calculate image luminance: {str(e)}')
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
                except Exception as e:
                    app.logger.warning(f'Could not delete file {source}: {str(e)}')
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
            except Exception as e:
                app.logger.warning(f'Could not delete file {path}: {str(e)}')
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
    try:
        row = CFG.query.filter_by(cfg=name).with_for_update().first()
        if row:
            row.value = value
        else:
            row = CFG(cfg=name, value=value)
            db.session.add(row)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        app.logger.error(f'set_cfg error for {name}: {str(e)}')


def ensure_column(table_name, column_name, column_sql):
    allowed_tables = {'Users', 'Session', 'Dish', 'MealOrder', 'PaymentOperation',
                     'Notification', 'CFG', 'FeedbackThread', 'FeedbackMessage',
                     'DishGroup', 'FamilyGroup', 'ParentInvite', 'ParentStudentLink',
                     'InventoryItem', 'PurchaseRequest', 'IncidentReport', 'Review'}

    if table_name not in allowed_tables:
        app.logger.error(f'ensure_column: Invalid table name: {table_name}')
        return

    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
        app.logger.error(f'ensure_column: Invalid column name: {column_name}')
        return

    allowed_patterns = [
        r'VARCHAR\(\d+\)',
        r'INTEGER',
        r'BOOLEAN',
        r'JSON',
        r'DATETIME',
        r'TEXT',
        r'FLOAT',
    ]

    if not any(re.search(pattern, column_sql) for pattern in allowed_patterns):
        app.logger.error(f'ensure_column: Invalid column_sql pattern: {column_sql}')
        return

    suspicious_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'UNION', 'SELECT', '--', ';']
    if any(keyword in column_sql.upper() for keyword in suspicious_keywords):
        app.logger.error(f'ensure_column: Suspicious SQL in column_sql: {column_sql}')
        return

    inspector = inspect(db.engine)
    columns = [c['name'] for c in inspector.get_columns(table_name)]
    if column_name not in columns:
        try:
            with db.engine.begin() as conn:
                conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_sql}'))
            app.logger.info(f'Added column {column_name} to {table_name}')
        except Exception as e:
            app.logger.error(f'ensure_column error: {str(e)}')


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

    announcement = form.get('announcement', '').strip()
    set_cfg('announcement', announcement[:500] if announcement else '')
    announcement_type = form.get('announcement_type', 'info').strip()
    if announcement_type not in ('info', 'warning', 'error'):
        announcement_type = 'info'
    set_cfg('announcement_type', announcement_type)

    low_balance_threshold = max(0, min(100000, to_int(form.get('low_balance_threshold', ''), DEFAULT_CFG['low_balance_threshold'])))
    set_cfg('low_balance_threshold', low_balance_threshold)
    topup_max_amount = max(1, min(1000000, to_int(form.get('topup_max_amount', ''), DEFAULT_CFG['topup_max_amount'])))
    set_cfg('topup_max_amount', topup_max_amount)

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


def send_email(to_email, subject, template, plain_text=None, **kwargs):
    if not cfg_bool('mail_enabled', False):
        return False
    if not app.config.get('MAIL_SERVER') or not app.config.get('MAIL_USERNAME'):
        return False
    try:
        message = Message(subject, sender=f"{get_cfg('Name')} <{get_cfg('mail_username')}>", recipients=[to_email])
        message.html = render_template(template, **kwargs)
        if plain_text is not None:
            message.body = plain_text
        mail.send(message)
        return True
    except Exception as e:
        app.logger.error(f"Email send failed: {str(e)}")
        return False


def create_email_verification(user):
    EmailVerification.query.filter_by(email=user.email, is_verified=False).delete()
    db.session.commit()

    token = secrets.token_urlsafe(48)
    expires_at = datetime.utcnow() + timedelta(hours=24)

    verification = EmailVerification(
        email=user.email,
        user_id=user.id,
        token=token,
        expires_at=expires_at,
        is_verified=False
    )
    db.session.add(verification)
    db.session.commit()

    host = request.host_url.rstrip('/')
    verification_url = f"{host}/auth/verify-email/{token}/"
    cancel_url = f"{host}/auth/cancel-registration/{user.url_code}/"
    expires_at_formatted = expires_at.strftime('%d.%m.%Y %H:%M') + ' UTC'
    user_name = f"{user.name} {user.surname}"
    plain_text = (
        f"Здравствуйте, {user_name}!\n\n"
        f"Спасибо за регистрацию в Умной столовой.\n"
        f"Для активации аккаунта перейдите по ссылке: {verification_url}\n\n"
        f"Ссылка действительна до {expires_at_formatted}.\n\n"
        f"Если хотите отменить регистрацию: {cancel_url}\n\n"
        f"Если вы не регистрировались, проигнорируйте это письмо."
    )
    send_email(
        user.email,
        "Умная столовая — подтверждение регистрации",
        'mail_verification.html',
        plain_text=plain_text,
        user_name=user_name,
        verification_url=verification_url,
        cancel_url=cancel_url,
        expires_at_formatted=expires_at_formatted,
        site_name=get_cfg('Name', 'Умная столовая')
    )

    return verification


def create_pending_password_change(user, new_password_hash):
    PendingPasswordChange.query.filter_by(user_id=user.id, is_used=False).update({'is_used': True})
    token = secrets.token_urlsafe(48)
    expires_at = datetime.utcnow() + timedelta(hours=1)
    pending = PendingPasswordChange(
        user_id=user.id,
        new_password_hash=new_password_hash,
        token=token,
        expires_at=expires_at,
        is_used=False,
    )
    db.session.add(pending)
    db.session.commit()
    confirm_url = f"{request.host_url.rstrip('/')}/password/change/confirm/{token}/"
    expires_str = expires_at.strftime('%d.%m.%Y %H:%M') + ' UTC'
    plain_text = (
        f"Здравствуйте, {user.name}!\n\n"
        f"Вы запросили смену пароля в Умной столовой.\n"
        f"Для подтверждения перейдите по ссылке: {confirm_url}\n\n"
        f"Ссылка действительна до {expires_str} (1 час).\n\n"
        f"Если вы не запрашивали смену пароля, проигнорируйте это письмо — текущий пароль останется без изменений."
    )
    send_email(
        user.email,
        "Умная столовая — подтверждение смены пароля",
        'mail_password_change_confirm.html',
        plain_text=plain_text,
        user_name=user.name,
        confirm_url=confirm_url,
        expires_str=expires_str,
        site_name=get_cfg('Name', 'Умная столовая'),
    )
    return pending


def apply_pending_password_change(token):
    pending = PendingPasswordChange.query.filter_by(token=token, is_used=False).first()
    if not pending:
        return (None, 'Ссылка подтверждения недействительна.')
    if datetime.utcnow() > pending.expires_at:
        pending.is_used = True
        db.session.commit()
        return (None, 'Ссылка подтверждения истекла. Запросите смену пароля заново.')
    user = db.session.get(Users, pending.user_id)
    if not user or not user.is_active:
        pending.is_used = True
        db.session.commit()
        return (None, 'Аккаунт недоступен.')
    user.psw = pending.new_password_hash
    user.url_code = gen_code()
    Session.query.filter_by(user_id=user.id, is_active=True).update({'is_active': False})
    for t, payload in list(session_cache.items()):
        if payload.get('user_id') == user.id:
            session_cache.pop(t, None)
    pending.is_used = True
    db.session.commit()
    app.logger.info(f'Password changed for user {user.id}')
    return (user, None)


def cleanup_expired_unverified_users():
    global _last_cleanup_time
    now = datetime.utcnow()
    if _last_cleanup_time is not None and (now - _last_cleanup_time).total_seconds() < 3600:
        return
    _last_cleanup_time = now
    try:
        expired = EmailVerification.query.filter(
            EmailVerification.is_verified == False,
            EmailVerification.expires_at < now
        ).all()
        for ev in expired:
            user = Users.query.get(ev.user_id) if ev.user_id else None
            if user and not user.is_active:
                Session.query.filter_by(user_id=user.id).delete()
                EmailVerification.query.filter_by(user_id=user.id).delete()
                db.session.delete(user)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        app.logger.error(f"cleanup_expired_unverified_users failed: {str(e)}")


def verify_email_token(token):
    """Verify email token and activate user. Returns (user, error_message)."""
    verification = EmailVerification.query.filter_by(token=token).first()

    if not verification:
        return None, "Неверная ссылка подтверждения"

    if verification.is_verified:
        return None, "Этот аккаунт уже был активирован"

    if datetime.utcnow() > verification.expires_at:
        return None, "Ссылка активации истекла. Запросите новое письмо на странице входа"

    user = Users.query.get(verification.user_id)
    if not user:
        return None, "Пользователь не найден"

    user.is_active = True
    verification.is_verified = True
    db.session.commit()

    return user, None


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


def mask_email(email):
    try:
        local, domain = email.split('@', 1)
        return local[:2] + '***' + '@' + domain
    except Exception:
        return email


def create_login_otp(user, ip_address):
    LoginOTP.query.filter_by(user_id=user.id, is_used=False).update({'is_used': True})
    db.session.commit()
    code = ''.join([str(secrets.randbelow(10)) for _ in range(6)])
    expires_at = datetime.utcnow() + timedelta(minutes=10)
    otp = LoginOTP(
        user_id=user.id,
        code=code,
        ip_address=ip_address,
        expires_at=expires_at,
    )
    db.session.add(otp)
    db.session.commit()
    expires_str = otp.expires_at.strftime('%H:%M') + ' UTC'
    plain_text = (
        f"Код подтверждения входа в Умную столовую: {code}\n\n"
        f"Код действителен 10 минут (до {expires_str}).\n"
        f"Запрос с IP: {ip_address}\n\n"
        f"Если это не вы, проигнорируйте письмо."
    )
    send_email(
        user.email,
        "Умная столовая — код подтверждения входа",
        'mail_login_otp.html',
        otp_code=code,
        user_name=user.name,
        ip_address=ip_address,
        expires_str=expires_str,
        site_name=get_cfg('Name', 'Умная столовая'),
        plain_text=plain_text,
    )
    return otp


def verify_login_otp(user_id, entered_code):
    otp = LoginOTP.query.filter_by(user_id=user_id, is_used=False).order_by(LoginOTP.created_at.desc()).first()
    if not otp:
        return (None, 'Код не найден. Запросите новый.')
    if datetime.utcnow() > otp.expires_at:
        otp.is_used = True
        db.session.commit()
        return (None, 'Код истёк. Запросите новый.')
    if otp.attempts >= otp.max_attempts:
        otp.is_used = True
        db.session.commit()
        return (None, 'Превышено число попыток. Запросите новый код.')
    if otp.code != entered_code.strip():
        otp.attempts += 1
        remaining = otp.max_attempts - otp.attempts
        db.session.commit()
        if remaining <= 0:
            otp.is_used = True
            db.session.commit()
            return (None, 'Неверный код. Попытки исчерпаны, запросите новый.')
        return (None, f'Неверный код. Осталось попыток: {remaining}')
    otp.is_used = True
    db.session.commit()
    return (db.session.get(Users, user_id), None)


def check_otp_resend_cooldown(user_id):
    otp = LoginOTP.query.filter_by(user_id=user_id).order_by(LoginOTP.created_at.desc()).first()
    if otp and (datetime.utcnow() - otp.created_at).total_seconds() < 60:
        return int(60 - (datetime.utcnow() - otp.created_at).total_seconds())
    return 0


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
            safe_title = html_module.escape(str(title))
            safe_body = html_module.escape(str(body))
            safe_link = html_module.escape(str(target_link))
            message.html = (
                f"<h3>{safe_title}</h3>"
                f"<p>{safe_body}</p>"
                f"<p><a href='{safe_link}'>Открыть в приложении</a></p>"
            )
            mail.send(message)
        except Exception as e:
            app.logger.error(f'Failed to send email notification to {recipient.email}: {str(e)}')


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
        'site_announcement': str(get_cfg('announcement', '') or '').strip(),
        'site_announcement_type': str(get_cfg('announcement_type', 'info') or 'info').strip(),
        'low_balance_threshold': to_int(get_cfg('low_balance_threshold', DEFAULT_CFG['low_balance_threshold']), DEFAULT_CFG['low_balance_threshold']),
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
            payer_users = Users.query.filter(Users.id.in_(payer_ids)).all()
            payer_map = {payer.id: build_child_display_name(payer) for payer in payer_users}

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
                'can_cancel': order.status == 'ordered',
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


def get_allergen_warnings(user, dish):
    if not user:
        return []
    raw = (user.dop_data or {}).get('allergies', '')
    allergens = normalize_rule_tokens(raw)
    if not allergens:
        return []
    explicit_dish_allergens = [x.strip().lower() for x in (dish.allergens or '').split(',') if x.strip()]
    if explicit_dish_allergens:
        return [a for a in allergens if a.lower() in explicit_dish_allergens]
    search_text = ' '.join([
        str(dish.title or ''),
        str(dish.composition or ''),
        str(dish.description or ''),
    ]).lower()
    return [a for a in allergens if a.lower() in search_text]


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
    blocked_dish_ids = []
    blocked_allergens = []
    for row in rows:
        allowed.extend(normalize_rule_tokens(row.allowed_products))
        required.extend(normalize_rule_tokens(row.required_products))
        forbidden.extend(normalize_rule_tokens(row.forbidden_products))
        raw_limits = json.loads(row.limits or '{}') if row.limits else {}
        blocked_dish_ids.extend(int(x) for x in raw_limits.get('blocked_dish_ids', []) if str(x).isdigit())
        blocked_allergens.extend(str(a).strip().lower() for a in raw_limits.get('blocked_allergens', []) if str(a).strip())
    return {
        'daily_limit': daily_limit,
        'allowed': normalize_rule_tokens(','.join(allowed)),
        'required': normalize_rule_tokens(','.join(required)),
        'forbidden': normalize_rule_tokens(','.join(forbidden)),
        'blocked_dish_ids': list(set(blocked_dish_ids)),
        'blocked_allergens': list(set(blocked_allergens)),
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


def check_dish_against_limits(dish, restrictions):
    if not restrictions:
        return ''
    blocked_ids = restrictions.get('blocked_dish_ids') or []
    if dish.id in blocked_ids:
        return 'Блюдо заблокировано родительским ограничением.'
    blocked_allergens = restrictions.get('blocked_allergens') or []
    if blocked_allergens:
        search_text = ' '.join([
            str(dish.title or ''),
            str(dish.composition or ''),
            str(dish.description or ''),
        ]).lower()
        for allergen in blocked_allergens:
            if allergen and allergen.lower() in search_text:
                return f'Блюдо содержит аллерген, заблокированный родителем: {allergen}'
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
def run_periodic_cleanup():
    cleanup_expired_unverified_users()


@app.before_request
def attach_user_to_request():
    token = request.cookies.get('session_token')
    user, error = check_session(token)
    g.current_user = user
    g.session_error = error


@app.before_request
def enforce_setup_gate():
    endpoint = request.endpoint or ''
    if endpoint in {'static', 'favicon', 'favicon_png', 'auth.setup'}:
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
    flash('Сессия устарела, повторите действие.', 'error')
    return redirect('/login/new/')


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


sys.modules.setdefault('main', sys.modules['__main__'])

from routes.auth import auth as auth_bp
from routes.menu import menu as menu_bp
from routes.orders import orders as orders_bp
from routes.profile import profile as profile_bp
from routes.admin import admin as admin_bp
from routes.kitchen import kitchen as kitchen_bp
from routes.misc import misc as misc_bp

app.register_blueprint(auth_bp)
app.register_blueprint(menu_bp)
app.register_blueprint(orders_bp)
app.register_blueprint(profile_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(kitchen_bp)
app.register_blueprint(misc_bp)


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
