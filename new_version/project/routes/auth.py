from flask import Blueprint, g, redirect, render_template, request, session, flash
from main import (
    db, app, get_csrf_token, is_valid_csrf_request, check_session,
    build_base_context, require_user, require_roles, message_page,
    Users, Session, EmailVerification, PasswordReset, LoginOTP,
    normalize_email, is_valid_email, find_user_by_email, resolve_super_admin_by_password,
    gen_code, sign_in_user, logout_user_response, create_email_verification, verify_email_token,
    create_password_reset, get_active_password_reset, send_email, get_cfg,
    REGISTRATION_ROLES, session_cache, generate_password_hash, check_password_hash,
    generate_setup_access_code, cfg_bool, DEFAULT_CFG, save_project_settings_from_request,
    ensure_super_admin, set_cfg, refresh_runtime_config, contact_data_to_raw,
    to_int, datetime,
    check_rate_limit, record_failed_attempt, clear_rate_limit,
    create_login_otp, verify_login_otp, check_otp_resend_cooldown, mask_email
)
auth = Blueprint('auth', __name__)


@auth.route('/setup/', methods=['GET', 'POST'])
def setup():
    if cfg_bool('setup_done', DEFAULT_CFG['setup_done']):
        return redirect('/')

    setup_unlocked = bool(session.get('setup_unlocked', False))
    mes = ''
    form_data = {}
    field_errors = {}

    if request.method == 'POST':
        form_data = {
            'site_name': request.form.get('site_name', ''),
            'school_name': request.form.get('school_name', ''),
            'contacts_raw': request.form.get('contacts_raw', ''),
            'host': request.form.get('host', ''),
            'port': request.form.get('port', ''),
            'protection': request.form.get('protection', ''),
            'mail_server': request.form.get('mail_server', ''),
            'mail_port': request.form.get('mail_port', ''),
            'mail_username': request.form.get('mail_username', ''),
            'admin_email': request.form.get('admin_email', ''),
            'bg_blur': request.form.get('bg_blur', '0'),
        }
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
                session.permanent = False
                session.modified = True
                app.logger.info(f'Setup unlocked from IP {request.remote_addr}')
                flash('Код доступа подтвержден.', 'success')
                return redirect('/setup/')
            else:
                app.logger.warning(f'Failed setup code attempt from IP {request.remote_addr}')
                mes = 'Неверный код доступа.'
        else:
            admin_email = normalize_email(request.form.get('admin_email', ''))
            admin_password = request.form.get('admin_password', '')
            admin_password_confirm = request.form.get('admin_password_confirm', '')

            if '@' not in admin_email:
                mes = 'Введите корректный email супер-администратора.'
                field_errors['admin_email'] = True
            elif len(admin_password) < 6:
                mes = 'Пароль супер-администратора должен быть не короче 6 символов.'
                field_errors['admin_password'] = True
            elif admin_password != admin_password_confirm:
                mes = 'Пароли супер-администратора не совпадают.'
                field_errors['admin_password'] = True
                field_errors['admin_password_confirm'] = True
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
            form_data=form_data,
            field_errors=field_errors,
            mes=mes,
            setup_unlocked=setup_unlocked,
            setup_hint=setup_hint,
        ),
    )


@auth.route('/reg/<cod>/', methods=['GET', 'POST'])
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
    form_data = {'email': '', 'user_name': '', 'user_surname': '', 'user_patronymic': '', 'role': 'student'}
    field_errors = {}
    if request.method == 'POST':
        form_data['email'] = request.form.get('email', '')
        form_data['user_name'] = request.form.get('user_name', '').strip()
        form_data['user_surname'] = request.form.get('user_surname', '').strip()
        form_data['user_patronymic'] = request.form.get('user_patronymic', '').strip()
        form_data['role'] = request.form.get('role', 'student')
        email = normalize_email(form_data['email'])
        password = request.form.get('password', '')
        check_password = request.form.get('check_password', '')
        name = form_data['user_name']
        surname = form_data['user_surname']
        otchestvo = form_data['user_patronymic']
        role = form_data['role']
        consent = bool(request.form.get('cbc'))
        email_notifications = bool(request.form.get('mail_notify_optin'))

        if not consent:
            mes = 'Необходимо согласие на обработку персональных данных.'
        elif not is_valid_email(email):
            mes = 'Введите корректный email.'
            field_errors['email'] = True
        elif password != check_password:
            mes = 'Пароли не совпадают.'
            field_errors['password'] = True
        elif len(password) < 6:
            mes = 'Пароль должен быть не короче 6 символов.'
            field_errors['password'] = True
        elif not name or not surname:
            mes = 'Введите имя и фамилию.'
            if not name:
                field_errors['user_name'] = True
            if not surname:
                field_errors['user_surname'] = True
        elif role not in REGISTRATION_ROLES:
            mes = 'Выберите корректную роль.'
            field_errors['role'] = True
        else:
            existing = find_user_by_email(email)
            if existing:
                mes = 'Этот email уже зарегистрирован.'
                field_errors['email'] = True
            else:
                user = Users(
                    email=email,
                    psw=generate_password_hash(password),
                    name=name,
                    surname=surname,
                    otchestvo=otchestvo,
                    registrating=False,
                    url_code=gen_code(),
                    role=role,
                    dop_data={'email_notifications': email_notifications},
                    balance=0,
                    is_active=False,
                )
                db.session.add(user)
                try:
                    db.session.commit()
                    if cfg_bool('mail_enabled', False):
                        create_email_verification(user)
                        return render_template('login_confimation.html',
                                               **build_base_context(None, email=email))
                    else:
                        user.is_active = True
                        db.session.commit()
                        return sign_in_user(user)
                except Exception:
                    db.session.rollback()
                mes = 'Ошибка при регистрации. Попробуйте позже.'

    return render_template('reg.html', **build_base_context(None, roles_for_registration=REGISTRATION_ROLES, mes=mes, form_data=form_data, field_errors=field_errors))


@auth.route('/cancel/<cod>/', methods=['GET'])
@auth.route('/cancel-registration/<cod>/', methods=['GET'])
def reg_cancel(cod):
    user = Users.query.filter_by(url_code=cod).first()
    if not user or user.is_active:
        return message_page('Ссылка отмены недействительна.')
    EmailVerification.query.filter_by(user_id=user.id).delete()
    Session.query.filter_by(user_id=user.id).delete()
    for token, payload in list(session_cache.items()):
        if payload.get('user_id') == user.id:
            session_cache.pop(token, None)
    db.session.delete(user)
    db.session.commit()
    return render_template('reg_cancelled.html', **build_base_context(None))


@auth.route('/login/<cod>/', methods=['GET', 'POST'])
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
    pending_verification_email = None
    form_data = {'email': ''}
    field_errors = {}
    if request.method == 'POST':
        ip = request.headers.get('X-Forwarded-For', request.remote_addr or '0.0.0.0').split(',')[0].strip()
        blocked_until, remaining = check_rate_limit(ip)
        if blocked_until:
            mes = f'Слишком много попыток. Повторите через {remaining} мин.'
            return render_template('login.html', **build_base_context(None, mes=mes, form_data=form_data, field_errors=field_errors))
        form_data['email'] = request.form.get('email', '')
        email = normalize_email(form_data['email'])
        password = request.form.get('password', '')
        user = find_user_by_email(email)
        if not user:
            user = resolve_super_admin_by_password(email, password)
        if not user or not check_password_hash(user.psw, password):
            mes = 'Неверный email или пароль.'
            field_errors['email'] = True
            record_failed_attempt(ip)
        elif not user.is_active:
            verification = EmailVerification.query.filter_by(user_id=user.id, is_verified=False).first()
            if verification and datetime.utcnow() < verification.expires_at:
                pending_verification_email = email
                mes = 'Аккаунт не активирован. Проверьте письмо с ссылкой активации.'
            else:
                mes = 'Аккаунт деактивирован.'
                record_failed_attempt(ip)
        elif not user.registrating:
            user.registrating = True
            db.session.commit()
            clear_rate_limit(ip)
            if not cfg_bool('mail_enabled', False):
                return sign_in_user(user)
            create_login_otp(user, ip)
            session['otp_user_id'] = user.id
            return redirect('/login/verify/')
        else:
            clear_rate_limit(ip)
            if not cfg_bool('mail_enabled', False):
                return sign_in_user(user)
            create_login_otp(user, ip)
            session['otp_user_id'] = user.id
            return redirect('/login/verify/')

    context = build_base_context(None, mes=mes, form_data=form_data, field_errors=field_errors)
    if pending_verification_email:
        context['pending_verification_email'] = pending_verification_email
    return render_template('login.html', **context)


@auth.route('/password/restore/', methods=['GET', 'POST'])
def password_restore():
    if g.current_user:
        return redirect('/')

    mes = ''
    reset_url = ''
    form_data = {'email': ''}
    field_errors = {}
    if request.method == 'POST':
        form_data['email'] = request.form.get('email', '')
        email = normalize_email(form_data['email'])
        generic_mes = 'Если аккаунт найден, инструкция по восстановлению отправлена на email.'
        if not is_valid_email(email):
            mes = 'Введите корректный email.'
            field_errors['email'] = True
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
        **build_base_context(None, mes=mes, reset_url=reset_url, form_data=form_data, field_errors=field_errors),
    )


@auth.route('/password/reset/<code>/', methods=['GET', 'POST'])
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


@auth.route('/auth/verify-email/<token>/', methods=['GET'])
def verify_email(token):
    if g.current_user:
        return redirect('/')

    user, error = verify_email_token(token)

    if error:
        return render_template('message.html', **build_base_context(None, message=error, title="Ошибка верификации"))

    return sign_in_user(user)


@auth.route('/auth/resend-verification/', methods=['GET', 'POST'])
def resend_verification():
    if g.current_user:
        return redirect('/')

    mes = ''
    form_data = {'email': ''}
    field_errors = {}
    if request.method == 'POST':
        form_data['email'] = request.form.get('email', '')
        email = normalize_email(form_data['email'])

        if not is_valid_email(email):
            mes = 'Введите корректный email.'
            field_errors['email'] = True
        else:
            user = find_user_by_email(email)

            if not user:
                mes = 'Если аккаунт найден, письмо отправлено на email.'
            elif user.is_active:
                mes = 'Этот аккаунт уже активирован. Войдите в систему.'
            else:
                try:
                    create_email_verification(user)
                    mes = 'Письмо с ссылкой активации отправлено на ваш email. Если письмо не приходит, проверьте папку спама.'
                    return render_template('message.html', **build_base_context(None, message=mes, title="Письмо отправлено"))
                except Exception as e:
                    app.logger.error(f"Resend verification failed: {str(e)}")
                    mes = 'Ошибка при отправке письма. Попробуйте позже.'

    return render_template('resend_verification.html', **build_base_context(None, mes=mes, form_data=form_data, field_errors=field_errors))


@auth.route('/login/verify/', methods=['GET', 'POST'])
def login_verify_otp():
    if g.current_user:
        return redirect('/')
    otp_user_id = session.get('otp_user_id')
    if not otp_user_id:
        return redirect('/login/new/')
    user = db.session.get(Users, otp_user_id)
    if not user or not user.is_active:
        session.pop('otp_user_id', None)
        return redirect('/login/new/')
    mes = ''
    resend_cooldown = check_otp_resend_cooldown(otp_user_id)
    if request.method == 'POST':
        action = request.form.get('action', '')
        if action == 'resend':
            cooldown = check_otp_resend_cooldown(otp_user_id)
            if cooldown > 0:
                mes = f'Повторная отправка доступна через {cooldown} сек.'
            else:
                ip = request.headers.get('X-Forwarded-For', request.remote_addr or '0.0.0.0').split(',')[0].strip()
                create_login_otp(user, ip)
                mes = 'Новый код отправлен на вашу почту.'
                resend_cooldown = 60
        else:
            entered_code = request.form.get('otp_code', '').strip()
            if not entered_code:
                mes = 'Введите код подтверждения.'
            else:
                verified_user, error = verify_login_otp(otp_user_id, entered_code)
                if error:
                    mes = error
                else:
                    session.pop('otp_user_id', None)
                    ip = request.headers.get('X-Forwarded-For', request.remote_addr or '0.0.0.0').split(',')[0].strip()
                    clear_rate_limit(ip)
                    return sign_in_user(verified_user)
    return render_template('login_verify_otp.html',
        **build_base_context(None, mes=mes, resend_cooldown=resend_cooldown, user_email_masked=mask_email(user.email)))


@auth.route('/logout/')
def logout():
    return logout_user_response()
