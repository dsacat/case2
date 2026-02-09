import os
import platform
import shlex
from datetime import datetime, timedelta
from time import sleep
from sqlalchemy import func


class CustomConsole:
    def __init__(self, CFG, Users, Session, db, app, USER_ROLES, extra_models=None, hooks=None, mode=False,
                 log_file='console.txt'):
        self.CFG = CFG
        self.Users = Users
        self.Session = Session
        self.db = db
        self.app = app
        self.USER_ROLES = USER_ROLES
        self.extra_models = extra_models or {}
        self.hooks = hooks or {}
        self.mode = mode
        self.log_file = log_file
        self.allowed_commands_view = None

        if mode:
            def printer(msg=''):
                with open(self.log_file, 'a', encoding='utf-8') as f:
                    f.write(str(msg) + '\n')
        else:
            def printer(msg=''):
                print(msg)
        self.print = printer

        self.command_templates = [
            {'title': 'Пользователи', 'command': 'list_users'},
            {'title': 'Роли', 'command': 'role_stats'},
            {'title': 'Сессии', 'command': 'sessions'},
            {'title': 'Блюда', 'command': 'list_dishes'},
            {'title': 'Обратная связь', 'command': 'feedback_stats'},
            {'title': 'Закупки', 'command': 'list_purchase_requests'},
            {'title': 'Сводка', 'command': 'stats'},
            {'title': 'Первичная настройка', 'command': 'setup_wizard'},
            {'title': 'Последние входы', 'command': 'recent_logins'},
            {'title': 'Список обращений', 'command': 'list_feedback'},
            {'title': 'Инвентарь', 'command': 'list_inventory'},
            {'title': 'Инциденты', 'command': 'list_incidents'},
            {'title': 'Заказы', 'command': 'list_orders'},
            {'title': 'Платежи', 'command': 'list_payments'},
            {'title': 'Уведомления', 'command': 'list_notifications'},
            {'title': 'Отзывы', 'command': 'list_reviews'},
            {'title': 'Статистика инцидентов', 'command': 'incident_stats'},
            {'title': 'Статистика заказов', 'command': 'order_stats'},
            {'title': 'Статистика платежей', 'command': 'payment_stats'},
            {'title': 'Статистика уведомлений', 'command': 'notification_stats'},
            {'title': 'Статистика отзывов', 'command': 'review_stats'},
            {'title': 'Статистика инвентаря', 'command': 'inventory_stats'},
            {'title': 'Система', 'command': 'system_info'},
            {'title': 'Сбросы паролей', 'command': 'list_password_resets'},
        ]

        self.commands = {
            'help': self.cmd_help,
            'clear': self.cmd_clear,
            'get_cfg': self.cmd_get_cfg,
            'set_cfg': self.cmd_set_cfg,
            'list_users': self.cmd_list_users,
            'user_info': self.cmd_user_info,
            'change_role': self.cmd_change_role,
            'activate_user': self.cmd_activate_user,
            'deactivate_user': self.cmd_deactivate_user,
            'delete_user': self.cmd_delete_user,
            'sessions': self.cmd_sessions,
            'kill_session': self.cmd_kill_session,
            'stats': self.cmd_stats,
            'role_stats': self.cmd_role_stats,
            'recent_logins': self.cmd_recent_logins,
            'list_dishes': self.cmd_list_dishes,
            'delete_dish': self.cmd_delete_dish,
            'feedback_stats': self.cmd_feedback_stats,
            'list_feedback': self.cmd_list_feedback,
            'list_purchase_requests': self.cmd_list_purchase_requests,
            'setup_wizard': self.cmd_setup_wizard,
            'system_info': self.cmd_system_info,
            'list_inventory': self.cmd_list_inventory,
            'inventory_stats': self.cmd_inventory_stats,
            'list_incidents': self.cmd_list_incidents,
            'incident_stats': self.cmd_incident_stats,
            'list_orders': self.cmd_list_orders,
            'order_stats': self.cmd_order_stats,
            'list_payments': self.cmd_list_payments,
            'payment_stats': self.cmd_payment_stats,
            'list_notifications': self.cmd_list_notifications,
            'notification_stats': self.cmd_notification_stats,
            'list_reviews': self.cmd_list_reviews,
            'review_stats': self.cmd_review_stats,
            'list_password_resets': self.cmd_list_password_resets,
            'reset_stats': self.cmd_reset_stats,
        }

    def start_console(self):
        with open(self.log_file, 'w', encoding='utf-8') as f:
            f.write('Консоль запущена.\n')

    def get_log(self):
        try:
            with open(self.log_file, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception:
            return ''

    def start(self):
        sleep(1)
        self.print('Консоль запущена.')
        self.cmd_help([])
        while True:
            try:
                line = input('>>> ').strip()
                if line.upper() == 'EXIT':
                    break
                self.execute_command(line, echo=False)
            except KeyboardInterrupt:
                self.print('Для выхода введите EXIT')
            except Exception as exc:
                self.print(f'Ошибка: {exc}')

    def execute_command(self, command_line, echo=False, allowed_commands=None):
        parts = shlex.split(command_line)
        if not parts:
            return self.get_log()
        command = parts[0].lower()
        args = parts[1:]

        if echo:
            self.print(f'>>> {command_line}')

        if allowed_commands is not None:
            allowed_set = {str(item).lower() for item in allowed_commands}
            if command not in allowed_set:
                self.print('Команда недоступна для вашей роли.')
                return self.get_log()
            self.allowed_commands_view = allowed_set

        handler = self.commands.get(command)
        if not handler:
            self.print('Неизвестная команда. Используйте help.')
            self.allowed_commands_view = None
            return self.get_log()

        with self.app.app_context():
            try:
                handler(args)
            except Exception as exc:
                self.print(f'Ошибка выполнения: {exc}')
            finally:
                self.allowed_commands_view = None
        return self.get_log()

    def __get_cfg(self, name, default=None):
        row = self.CFG.query.filter_by(cfg=name).first()
        return row.value if row else default

    def __set_cfg(self, name, value):
        row = self.CFG.query.filter_by(cfg=name).first()
        if row:
            row.value = value
        else:
            row = self.CFG(cfg=name, value=value)
            self.db.session.add(row)
        self.db.session.commit()

    def cmd_help(self, args):
        commands_help = {
            'help': 'Справка',
            'clear': 'Очистить экран/лог',
            'get_cfg <name>': 'Показать значение конфигурации',
            'set_cfg <name> <value>': 'Изменить конфигурацию',
            'list_users [limit]': 'Список пользователей',
            'user_info <id>': 'Информация о пользователе',
            'change_role <id> <role>': 'Изменить роль',
            'activate_user <id>': 'Активировать пользователя',
            'deactivate_user <id>': 'Деактивировать пользователя',
            'delete_user <id>': 'Мягко удалить пользователя',
            'sessions': 'Активные сессии',
            'kill_session <id>': 'Завершить сессию',
            'stats': 'Общая статистика',
            'role_stats': 'Статистика по ролям',
            'recent_logins [limit]': 'Последние входы',
            'list_dishes [limit]': 'Список блюд',
            'delete_dish <id>': 'Скрыть блюдо',
            'feedback_stats': 'Статистика обращений',
            'list_feedback [limit]': 'Список обращений',
            'list_purchase_requests [limit]': 'Список заявок на закупку',
            'setup_wizard': 'Перезапустить первичную настройку',
            'system_info': 'Информация о системе',
            'list_inventory [limit]': 'Список складских позиций',
            'inventory_stats': 'Статистика по складу',
            'list_incidents [limit]': 'Список инцидентов',
            'incident_stats': 'Статистика инцидентов',
            'list_orders [limit]': 'Список заказов',
            'order_stats': 'Статистика заказов',
            'list_payments [limit]': 'Список платежей',
            'payment_stats': 'Статистика платежей',
            'list_notifications [limit]': 'Список уведомлений',
            'notification_stats': 'Статистика уведомлений',
            'list_reviews [limit]': 'Список отзывов',
            'review_stats': 'Статистика отзывов',
            'list_password_resets [limit]': 'Список запросов сброса пароля',
            'reset_stats': 'Статистика сбросов пароля',
            'EXIT': 'Выход',
        }
        self.print('Доступные команды:')
        allowed = self.allowed_commands_view
        for cmd, desc in commands_help.items():
            if allowed is not None:
                cmd_name = cmd.split(' ', 1)[0].strip().lower()
                if cmd_name not in allowed:
                    continue
            self.print(f'  {cmd:<34} {desc}')

    def cmd_clear(self, args):
        if self.mode:
            self.start_console()
        else:
            os.system('cls' if os.name == 'nt' else 'clear')

    def cmd_get_cfg(self, args):
        if not args:
            self.print('Использование: get_cfg <name>')
            return
        self.print(f'{args[0]}: {self.__get_cfg(args[0])}')

    def cmd_set_cfg(self, args):
        if len(args) < 2:
            self.print('Использование: set_cfg <name> <value>')
            return
        name = args[0]
        value = ' '.join(args[1:])
        if value.isdigit():
            value = int(value)
        elif value.lower() in {'true', 'false'}:
            value = value.lower() == 'true'
        self.__set_cfg(name, value)
        self.print(f'{name} = {value}')

    def cmd_list_users(self, args):
        limit = int(args[0]) if args and args[0].isdigit() else 100
        users = self.Users.query.order_by(self.Users.id.desc()).limit(limit).all()
        self.print(f"{'ID':<5} {'Email':<30} {'Role':<14} {'Active':<8} {'Balance':<8}")
        self.print('-' * 72)
        for user in users:
            self.print(f"{user.id:<5} {user.email[:29]:<30} {user.role:<14} {str(user.is_active):<8} {user.balance:<8}")

    def cmd_user_info(self, args):
        if not args or not args[0].isdigit():
            self.print('Использование: user_info <id>')
            return
        user = self.db.session.get(self.Users, int(args[0]))
        if not user:
            self.print('Пользователь не найден.')
            return
        self.print(f'ID: {user.id}')
        self.print(f'Email: {user.email}')
        self.print(f'ФИО: {user.surname} {user.name} {user.otchestvo}')
        self.print(f'Роль: {user.role}')
        self.print(f'Активен: {user.is_active}')
        self.print(f'Баланс: {user.balance}')
        self.print(f'Создан: {user.created_at}')
        self.print(f'Последний вход: {user.last_login}')

    def cmd_change_role(self, args):
        if len(args) < 2 or not args[0].isdigit():
            self.print('Использование: change_role <id> <role>')
            self.print('Доступные роли: ' + ', '.join(self.USER_ROLES.keys()))
            return
        user = self.db.session.get(self.Users, int(args[0]))
        new_role = args[1]
        if not user:
            self.print('Пользователь не найден.')
            return
        if new_role not in self.USER_ROLES:
            self.print('Некорректная роль.')
            return
        old_role = user.role
        user.role = new_role
        self.db.session.commit()
        self.print(f'Роль изменена: {old_role} -> {new_role}')

    def cmd_activate_user(self, args):
        if not args or not args[0].isdigit():
            self.print('Использование: activate_user <id>')
            return
        user = self.db.session.get(self.Users, int(args[0]))
        if not user:
            self.print('Пользователь не найден.')
            return
        user.is_active = True
        self.db.session.commit()
        self.print('Пользователь активирован.')

    def cmd_deactivate_user(self, args):
        if not args or not args[0].isdigit():
            self.print('Использование: deactivate_user <id>')
            return
        user = self.db.session.get(self.Users, int(args[0]))
        if not user:
            self.print('Пользователь не найден.')
            return
        user.is_active = False
        self.Session.query.filter_by(user_id=user.id, is_active=True).update({'is_active': False})
        self.db.session.commit()
        self.print('Пользователь деактивирован.')

    def cmd_delete_user(self, args):
        if not args or not args[0].isdigit():
            self.print('Использование: delete_user <id>')
            return
        user = self.db.session.get(self.Users, int(args[0]))
        if not user:
            self.print('Пользователь не найден.')
            return
        user.is_active = False
        user.email = f"deleted_{user.id}_{int(datetime.utcnow().timestamp())}@local"
        self.Session.query.filter_by(user_id=user.id, is_active=True).update({'is_active': False})
        self.db.session.commit()
        self.print('Пользователь деактивирован и обезличен.')

    def cmd_sessions(self, args):
        sessions = self.Session.query.filter_by(is_active=True).order_by(self.Session.last_seen.desc()).limit(200).all()
        self.print(f"{'ID':<5} {'User':<6} {'IP':<16} {'Expires':<20}")
        self.print('-' * 52)
        for sess in sessions:
            self.print(f"{sess.id:<5} {sess.user_id:<6} {(sess.ip_address or '-')[:15]:<16} {str(sess.expires_at)[:19]:<20}")

    def cmd_kill_session(self, args):
        if not args or not args[0].isdigit():
            self.print('Использование: kill_session <id>')
            return
        sess = self.db.session.get(self.Session, int(args[0]))
        if not sess:
            self.print('Сессия не найдена.')
            return
        sess.is_active = False
        self.db.session.commit()
        self.print('Сессия завершена.')

    def cmd_stats(self, args):
        Dish = self.extra_models.get('Dish')
        FeedbackThread = self.extra_models.get('FeedbackThread')
        PurchaseRequest = self.extra_models.get('PurchaseRequest')
        MealOrder = self.extra_models.get('MealOrder')

        self.print('Сводка:')
        self.print(f"Пользователи: {self.Users.query.count()}")
        self.print(f"Активные сессии: {self.Session.query.filter_by(is_active=True).count()}")
        if Dish:
            self.print(f"Блюда: {Dish.query.count()}")
        if MealOrder:
            self.print(f"Заказы: {MealOrder.query.count()}")
        if FeedbackThread:
            self.print(f"Обращения: {FeedbackThread.query.count()}")
        if PurchaseRequest:
            self.print(f"Заявки на закупку: {PurchaseRequest.query.count()}")

    def cmd_role_stats(self, args):
        for role in self.USER_ROLES:
            count = self.Users.query.filter_by(role=role, is_active=True).count()
            self.print(f'{role}: {count}')

    def cmd_recent_logins(self, args):
        limit = int(args[0]) if args and args[0].isdigit() else 20
        users = (self.Users.query
                 .filter(self.Users.last_login.isnot(None))
                 .order_by(self.Users.last_login.desc())
                 .limit(limit)
                 .all())
        self.print(f"{'Email':<30} {'Role':<14} {'Last login':<20}")
        self.print('-' * 66)
        for user in users:
            self.print(f"{user.email[:29]:<30} {user.role:<14} {str(user.last_login)[:19]:<20}")

    def cmd_list_dishes(self, args):
        Dish = self.extra_models.get('Dish')
        if not Dish:
            self.print('Модель блюд не подключена.')
            return
        limit = int(args[0]) if args and args[0].isdigit() else 100
        rows = Dish.query.order_by(Dish.id.desc()).limit(limit).all()
        self.print(f"{'ID':<5} {'Title':<30} {'Price':<8} {'Active':<8}")
        self.print('-' * 58)
        for row in rows:
            self.print(f"{row.id:<5} {row.title[:29]:<30} {row.price:<8} {str(row.is_active):<8}")

    def cmd_delete_dish(self, args):
        Dish = self.extra_models.get('Dish')
        if not Dish:
            self.print('Модель блюд не подключена.')
            return
        if not args or not args[0].isdigit():
            self.print('Использование: delete_dish <id>')
            return
        row = self.db.session.get(Dish, int(args[0]))
        if not row:
            self.print('Блюдо не найдено.')
            return
        row.is_active = False
        row.updated_at = datetime.utcnow()
        self.db.session.commit()
        self.print('Блюдо скрыто.')

    def cmd_feedback_stats(self, args):
        FeedbackThread = self.extra_models.get('FeedbackThread')
        if not FeedbackThread:
            self.print('Модель обратной связи не подключена.')
            return
        open_count = FeedbackThread.query.filter_by(status='open').count()
        closed_count = FeedbackThread.query.filter_by(status='closed').count()
        self.print(f'Открыто: {open_count}')
        self.print(f'Закрыто: {closed_count}')
        self.print(f'Всего: {open_count + closed_count}')

    def cmd_list_feedback(self, args):
        FeedbackThread = self.extra_models.get('FeedbackThread')
        if not FeedbackThread:
            self.print('Модель обратной связи не подключена.')
            return
        limit = int(args[0]) if args and args[0].isdigit() else 50
        rows = FeedbackThread.query.order_by(FeedbackThread.updated_at.desc()).limit(limit).all()
        self.print(f"{'ID':<5} {'User':<6} {'Status':<10} {'Subject':<40}")
        self.print('-' * 64)
        for row in rows:
            self.print(f"{row.id:<5} {row.user_id:<6} {row.status:<10} {row.subject[:39]:<40}")

    def cmd_list_purchase_requests(self, args):
        PurchaseRequest = self.extra_models.get('PurchaseRequest')
        if not PurchaseRequest:
            self.print('Модель закупок не подключена.')
            return
        limit = int(args[0]) if args and args[0].isdigit() else 50
        rows = PurchaseRequest.query.order_by(PurchaseRequest.created_at.desc()).limit(limit).all()
        self.print(f"{'ID':<5} {'Item':<24} {'Qty':<12} {'Cost':<10} {'Status':<10}")
        self.print('-' * 68)
        for row in rows:
            qty = f"{row.quantity:.2f} {row.unit}"
            self.print(f"{row.id:<5} {row.item_name[:23]:<24} {qty:<12} {row.expected_cost:<10} {row.status:<10}")

    def cmd_setup_wizard(self, args):
        callback = self.hooks.get('setup_wizard')
        if not callback:
            self.print('Команда недоступна.')
            return
        callback()
        self.print('Код доступа к веб-мастеру первичной настройки обновлен.')

    def cmd_system_info(self, args):
        self.print('Система:')
        self.print(f'OS: {platform.system()} {platform.release()}')
        self.print(f'Python: {platform.python_version()}')
        self.print(f'Platform: {platform.platform()}')

    def cmd_list_inventory(self, args):
        InventoryItem = self.extra_models.get('InventoryItem')
        if not InventoryItem:
            self.print('Модель склада не подключена.')
            return
        limit = int(args[0]) if args and args[0].isdigit() else 100
        rows = InventoryItem.query.order_by(InventoryItem.updated_at.desc()).limit(limit).all()
        self.print(f"{'ID':<5} {'Item':<28} {'Qty':<14} {'Min':<10} {'Updated':<19}")
        self.print('-' * 78)
        for row in rows:
            qty = f"{row.quantity:.2f} {row.unit}"
            minq = f"{row.min_quantity:.2f} {row.unit}"
            self.print(f"{row.id:<5} {row.name[:27]:<28} {qty:<14} {minq:<10} {str(row.updated_at)[:19]:<19}")

    def cmd_inventory_stats(self, args):
        InventoryItem = self.extra_models.get('InventoryItem')
        if not InventoryItem:
            self.print('Модель склада не подключена.')
            return
        total = InventoryItem.query.count()
        low = InventoryItem.query.filter(InventoryItem.quantity <= InventoryItem.min_quantity).count()
        self.print(f'Позиций: {total}')
        self.print(f'Ниже минимума: {low}')

    def cmd_list_incidents(self, args):
        Incident = self.extra_models.get('Incident')
        if not Incident:
            self.print('Модель инцидентов не подключена.')
            return
        limit = int(args[0]) if args and args[0].isdigit() else 80
        rows = Incident.query.order_by(Incident.created_at.desc()).limit(limit).all()
        self.print(f"{'ID':<5} {'Kind':<10} {'Status':<9} {'Severity':<10} {'Title':<32}")
        self.print('-' * 74)
        for row in rows:
            self.print(f"{row.id:<5} {row.kind[:9]:<10} {row.status:<9} {row.severity:<10} {row.title[:31]:<32}")

    def cmd_incident_stats(self, args):
        Incident = self.extra_models.get('Incident')
        if not Incident:
            self.print('Модель инцидентов не подключена.')
            return
        total = Incident.query.count()
        open_count = Incident.query.filter_by(status='open').count()
        resolved = Incident.query.filter_by(status='resolved').count()
        self.print(f'Инциденты всего: {total}')
        self.print(f'Открытые: {open_count}')
        self.print(f'Закрытые: {resolved}')
        rows = (Incident.query
                .with_entities(Incident.severity, func.count(Incident.id))
                .group_by(Incident.severity)
                .all())
        for severity, count in rows:
            self.print(f'{severity}: {count}')

    def cmd_list_orders(self, args):
        MealOrder = self.extra_models.get('MealOrder')
        if not MealOrder:
            self.print('Модель заказов не подключена.')
            return
        limit = int(args[0]) if args and args[0].isdigit() else 100
        rows = MealOrder.query.order_by(MealOrder.created_at.desc()).limit(limit).all()
        self.print(f"{'ID':<5} {'User':<6} {'Dish':<6} {'Status':<10} {'Price':<8} {'Created':<19}")
        self.print('-' * 82)
        for row in rows:
            self.print(f"{row.id:<5} {row.user_id:<6} {row.dish_id:<6} {row.status:<10} {row.price:<8} {str(row.created_at)[:19]:<19}")

    def cmd_order_stats(self, args):
        MealOrder = self.extra_models.get('MealOrder')
        if not MealOrder:
            self.print('Модель заказов не подключена.')
            return
        total = MealOrder.query.count()
        self.print(f'Заказов всего: {total}')
        rows = (MealOrder.query
                .with_entities(MealOrder.status, func.count(MealOrder.id))
                .group_by(MealOrder.status)
                .all())
        for status, count in rows:
            self.print(f'{status}: {count}')

    def cmd_list_payments(self, args):
        PaymentOperation = self.extra_models.get('PaymentOperation')
        if not PaymentOperation:
            self.print('Модель платежей не подключена.')
            return
        limit = int(args[0]) if args and args[0].isdigit() else 80
        rows = PaymentOperation.query.order_by(PaymentOperation.created_at.desc()).limit(limit).all()
        self.print(f"{'ID':<5} {'User':<6} {'Kind':<14} {'Amount':<10} {'Created':<19}")
        self.print('-' * 72)
        for row in rows:
            self.print(f"{row.id:<5} {row.user_id:<6} {row.kind[:13]:<14} {row.amount:<10} {str(row.created_at)[:19]:<19}")

    def cmd_payment_stats(self, args):
        PaymentOperation = self.extra_models.get('PaymentOperation')
        if not PaymentOperation:
            self.print('Модель платежей не подключена.')
            return
        total = PaymentOperation.query.count()
        self.print(f'Операций всего: {total}')
        rows = (PaymentOperation.query
                .with_entities(PaymentOperation.kind, func.count(PaymentOperation.id), func.sum(PaymentOperation.amount))
                .group_by(PaymentOperation.kind)
                .all())
        for kind, count, amount in rows:
            self.print(f'{kind}: {count} на сумму {amount or 0}')

    def cmd_list_notifications(self, args):
        Notification = self.extra_models.get('Notification')
        if not Notification:
            self.print('Модель уведомлений не подключена.')
            return
        limit = int(args[0]) if args and args[0].isdigit() else 80
        rows = Notification.query.order_by(Notification.created_at.desc()).limit(limit).all()
        self.print(f"{'ID':<5} {'User':<6} {'Read':<6} {'Title':<34} {'Created':<19}")
        self.print('-' * 80)
        for row in rows:
            self.print(f"{row.id:<5} {row.user_id:<6} {str(row.is_read):<6} {row.title[:33]:<34} {str(row.created_at)[:19]:<19}")

    def cmd_notification_stats(self, args):
        Notification = self.extra_models.get('Notification')
        if not Notification:
            self.print('Модель уведомлений не подключена.')
            return
        total = Notification.query.count()
        unread = Notification.query.filter_by(is_read=False).count()
        self.print(f'Уведомлений всего: {total}')
        self.print(f'Непрочитанные: {unread}')

    def cmd_list_reviews(self, args):
        DishReview = self.extra_models.get('DishReview')
        if not DishReview:
            self.print('Модель отзывов не подключена.')
            return
        limit = int(args[0]) if args and args[0].isdigit() else 80
        rows = DishReview.query.order_by(DishReview.created_at.desc()).limit(limit).all()
        self.print(f"{'ID':<5} {'Dish':<6} {'User':<6} {'Rate':<6} {'Created':<19}")
        self.print('-' * 62)
        for row in rows:
            self.print(f"{row.id:<5} {row.dish_id:<6} {row.user_id:<6} {row.rating:<6} {str(row.created_at)[:19]:<19}")

    def cmd_review_stats(self, args):
        DishReview = self.extra_models.get('DishReview')
        if not DishReview:
            self.print('Модель отзывов не подключена.')
            return
        total = DishReview.query.count()
        avg = self.db.session.query(func.avg(DishReview.rating)).scalar() or 0
        self.print(f'Отзывов всего: {total}')
        self.print(f'Средний рейтинг: {round(avg, 2)}')

    def cmd_list_password_resets(self, args):
        PasswordReset = self.extra_models.get('PasswordReset')
        if not PasswordReset:
            self.print('Модель сбросов не подключена.')
            return
        limit = int(args[0]) if args and args[0].isdigit() else 80
        rows = PasswordReset.query.order_by(PasswordReset.created_at.desc()).limit(limit).all()
        self.print(f"{'ID':<5} {'User':<6} {'Used':<6} {'Expires':<19} {'Code':<14}")
        self.print('-' * 68)
        for row in rows:
            code = (row.code or '')[:12]
            self.print(f"{row.id:<5} {row.user_id:<6} {str(row.is_used):<6} {str(row.expires_at)[:19]:<19} {code:<14}")

    def cmd_reset_stats(self, args):
        PasswordReset = self.extra_models.get('PasswordReset')
        if not PasswordReset:
            self.print('Модель сбросов не подключена.')
            return
        now = datetime.utcnow()
        total = PasswordReset.query.count()
        active = PasswordReset.query.filter(PasswordReset.expires_at > now, PasswordReset.is_used == False).count()
        used = PasswordReset.query.filter_by(is_used=True).count()
        self.print(f'Сбросов всего: {total}')
        self.print(f'Активные: {active}')
        self.print(f'Использованы: {used}')
