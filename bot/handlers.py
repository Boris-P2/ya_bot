import io
import csv
import asyncio
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import json
import logging

from database.session import SessionLocal
from database import crud, models
from shared.config import settings
from collector.main import DataCollector

logger = logging.getLogger(__name__)


# ========== КЛАВИАТУРЫ ==========

def get_main_keyboard(is_authorized: bool = False):
    """Главная клавиатура (меню)"""
    keyboard = []
    
    if not is_authorized:
        keyboard.append([InlineKeyboardButton("🔐 Войти по номеру телефона", callback_data="auth")])
    else:
        keyboard.append([
            InlineKeyboardButton("📞 Пригласить водителя", callback_data="invite"),
            InlineKeyboardButton("👥 Мои приглашения", callback_data="my_referrals")
        ])
        keyboard.append([
            InlineKeyboardButton("💰 Статус наград", callback_data="referral_stats"),
            InlineKeyboardButton("📊 Статистика", callback_data="stats")
        ])
        keyboard.append([
            InlineKeyboardButton("🏆 Топ водителей", callback_data="top"),
            InlineKeyboardButton("🔍 Поиск", callback_data="search")
        ])
        keyboard.append([
            InlineKeyboardButton("❓ Помощь", callback_data="help"),
            InlineKeyboardButton("🚪 Выйти", callback_data="logout")
        ])
    
    return InlineKeyboardMarkup(keyboard)


def get_back_keyboard():
    """Клавиатура с кнопкой 'Назад'"""
    keyboard = [[InlineKeyboardButton("◀️ Назад в меню", callback_data="menu")]]
    return InlineKeyboardMarkup(keyboard)


def get_auth_button_keyboard():
    """Клавиатура с кнопкой для входа (после согласия)"""
    keyboard = [[InlineKeyboardButton("🔐 Войти по номеру телефона", callback_data="auth")]]
    return InlineKeyboardMarkup(keyboard)


# ========== ОСНОВНЫЕ КОМАНДЫ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start - приветствие и запрос согласия"""
    user = update.effective_user
    
    db = SessionLocal()
    try:
        crud.add_or_update_user(
            db,
            telegram_id=user.id,
            username=user.username,
            is_admin=user.id in settings.ADMIN_IDS
        )
        
        user_record = crud.get_user(db, user.id)
        has_consent = user_record and user_record.consent_given == 1
        
        if has_consent:
            driver = crud.get_driver_by_telegram_id(db, user.id)
            is_authorized = driver is not None
            
            if is_authorized:
                await update.message.reply_text(
                    f"👋 С возвращением, {user.first_name}!",
                    reply_markup=get_main_keyboard(is_authorized=True)
                )
            else:
                welcome_text = (
                    f"👋 *Привет, {user.first_name}!*\n\n"
                    f"🤝 Этот бот помогает приглашать водителей в парк Яндекс.Такси.\n\n"
                    f"💰 *Как это работает:*\n"
                    f"• Вы приглашаете водителя по номеру телефона\n"
                    f"• Когда приглашённый водитель выполнит 100 заказов\n"
                    f"• Вы получаете бонусное вознаграждение!\n\n"
                    f"📱 *Для начала работы:*\n"
                    f"• Нажмите кнопку «Войти по номеру телефона»\n"
                    f"• Введите ваш номер телефона\n\n"
                    f"Приглашайте водителей и зарабатывайте бонусы!"
                )
                await update.message.reply_text(
                    welcome_text,
                    parse_mode='Markdown',
                    reply_markup=get_auth_button_keyboard()
                )
        else:
            await request_consent(update, context)
            
    finally:
        db.close()


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Возврат в главное меню"""
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    
    db = SessionLocal()
    try:
        driver = crud.get_driver_by_telegram_id(db, user.id)
        is_authorized = driver is not None
    finally:
        db.close()
    
    await query.edit_message_text(
        "📋 *Главное меню*\n\nВыберите действие:",
        parse_mode='Markdown',
        reply_markup=get_main_keyboard(is_authorized)
    )


async def request_consent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрашивает согласие на обработку данных"""
    user = update.effective_user
    
    db = SessionLocal()
    try:
        user_record = crud.get_user(db, user.id)
        if user_record and user_record.consent_given == 1:
            await auth_button(update, context)
            return
    finally:
        db.close()
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Да, я согласен", callback_data="consent_yes"),
            InlineKeyboardButton("❌ Нет, я отказываюсь", callback_data="consent_no")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    consent_text = (
        "🔐 *Согласие на обработку персональных данных*\n\n"
        "Для продолжения работы с ботом необходимо дать согласие на обработку следующих данных:\n"
        "• Номер телефона\n"
        "• ID водителя в системе Яндекс.Такси\n"
        "• Информация о количестве выполненных заказов\n"
        "• Статус работы водителя\n\n"
        "Ваши данные используются только для:\n"
        "• Отслеживания реферальных приглашений\n"
        "• Начисления бонусов за приглашения\n"
        "• Статистики работы парка\n\n"
        "Данные не передаются третьим лицам.\n\n"
        "📅 Дата и время согласия будут зафиксированы по Московскому времени.\n\n"
        "Вы можете отозвать согласие в любое время через команду /revoke_consent"
    )
    
    await update.message.reply_text(
        consent_text,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )


# ========== АВТОРИЗАЦИЯ ==========

async def auth_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия кнопки 'Войти по номеру телефона'"""
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        message = query.message
    else:
        message = update.message
    
    user = update.effective_user
    db = SessionLocal()
    try:
        user_record = crud.get_user(db, user.id)
        has_consent = user_record and user_record.consent_given == 1
        
        if not has_consent:
            await message.reply_text(
                "❌ Вы не дали согласие на обработку данных.\n\n"
                "Пожалуйста, нажмите /start и дайте согласие для продолжения.",
                reply_markup=get_back_keyboard()
            )
            return
    finally:
        db.close()
    
    context.user_data['awaiting_auth_phone'] = True
    
    text_msg = (
        "🔐 *Вход по номеру телефона*\n\n"
        "Пожалуйста, введите ваш номер телефона в формате:\n"
        "`+79001234567`\n\n"
        "Номер должен совпадать с номером, указанным в профиле Яндекс.Такси"
    )
    
    if update.callback_query:
        await query.edit_message_text(
            text_msg,
            parse_mode='Markdown',
            reply_markup=get_back_keyboard()
        )
    else:
        await message.reply_text(
            text_msg,
            parse_mode='Markdown',
            reply_markup=get_back_keyboard()
        )


async def handle_consent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ответа на запрос согласия"""
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    
    if query.data == "consent_yes":
        db = SessionLocal()
        try:
            crud.update_user_consent(db, user.id, True)
            
            user_record = crud.get_user(db, user.id)
            
            moscow_time = user_record.consent_date + timedelta(hours=3) if user_record and user_record.consent_date else None
            time_str = moscow_time.strftime('%d.%m.%Y %H:%M:%S') if moscow_time else 'неизвестно'
            
            await query.edit_message_text(
                f"✅ *Согласие получено!*\n\n"
                f"📅 Дата и время (МСК): {time_str}\n\n"
                f"Благодарим за доверие!\n\n"
                f"👇 *Нажмите кнопку ниже для входа в систему* 👇",
                parse_mode='Markdown',
                reply_markup=get_auth_button_keyboard()
            )
        finally:
            db.close()
        
    elif query.data == "consent_no":
        db = SessionLocal()
        try:
            crud.update_user_consent(db, user.id, False)
        finally:
            db.close()
        
        await query.edit_message_text(
            "❌ *Вы отказались от обработки данных.*\n\n"
            "К сожалению, без вашего согласия мы не можем предоставить вам доступ к функциям бота.\n\n"
            "Если вы передумаете, нажмите /start и дайте согласие.",
            parse_mode='Markdown'
        )
        return


async def handle_auth_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода номера телефона для авторизации"""
    if not context.user_data.get('awaiting_auth_phone'):
        return
    
    user = update.effective_user
    phone = update.message.text.strip()
    phone_clean = phone.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
    
    db = SessionLocal()
    try:
        driver = crud.get_driver_by_phone(db, phone_clean)
        
        if not driver:
            await update.message.reply_text(
                f"❌ Водитель с номером {phone} не найден в базе данных.\n\n"
                f"Убедитесь, что:\n"
                f"1. Номер указан в формате +79001234567\n"
                f"2. Ваш номер телефона есть в базе (он обновляется раз в сутки)\n\n"
                f"Попробуйте снова или нажмите 'Назад'.",
                reply_markup=get_back_keyboard()
            )
            return
        
        existing = crud.get_driver_by_telegram_id(db, user.id)
        if existing and existing.driver_id != driver.driver_id:
            await update.message.reply_text(
                f"⚠️ Ваш Telegram аккаунт уже привязан к другому водителю.\n"
                f"Свяжитесь с администратором для смены привязки.",
                reply_markup=get_back_keyboard()
            )
            return
        
        driver.telegram_id = user.id
        db.commit()
        
        context.user_data.pop('awaiting_auth_phone', None)
        
        await update.message.reply_text(
            f"✅ *Аккаунт привязан!*\n\n"
            f"👤 Водитель: {driver.last_name or 'Без имени'}\n"
            f"📞 Телефон: {driver.phone}\n"
            f"📦 Заказов: {driver.orders_count}\n\n"
            f"Теперь вам доступны все функции бота!",
            parse_mode='Markdown',
            reply_markup=get_main_keyboard(is_authorized=True)
        )
        
    finally:
        db.close()


# ========== ПРИГЛАШЕНИЯ ==========

async def invite_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия кнопки 'Пригласить водителя'"""
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    
    db = SessionLocal()
    try:
        driver = crud.get_driver_by_telegram_id(db, user.id)
        
        if not driver:
            await query.edit_message_text(
                "❌ Вы не авторизованы.\n\n"
                "Сначала нажмите кнопку «Войти по номеру телефона»",
                reply_markup=get_back_keyboard()
            )
            return
        
        context.user_data['awaiting_invite_phone'] = True
        
        await query.edit_message_text(
            "📞 *Пригласить водителя*\n\n"
            "Введите номер телефона водителя, которого хотите пригласить:\n"
            "`+79001234567`\n\n"
            "_Приглашать можно только водителей, зарегистрированных в течение последних 3 дней_\n\n"
            "⚠️ Лимит: не более 3 одновременных ожидающих приглашений",
            parse_mode='Markdown',
            reply_markup=get_back_keyboard()
        )
        
    finally:
        db.close()


async def handle_invite_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода номера телефона для приглашения"""
    if not context.user_data.get('awaiting_invite_phone'):
        return
    
    user = update.effective_user
    target_phone = update.message.text.strip()
    target_phone_clean = target_phone.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
    
    db = SessionLocal()
    try:
        driver = crud.get_driver_by_telegram_id(db, user.id)
        
        if not driver:
            await update.message.reply_text(
                "❌ Вы не авторизованы.\n"
                "Используйте кнопку «Войти по номеру телефона»",
                reply_markup=get_back_keyboard()
            )
            context.user_data.pop('awaiting_invite_phone', None)
            return
        
        if driver.phone == target_phone_clean:
            await update.message.reply_text(
                "❌ Нельзя пригласить самого себя",
                reply_markup=get_back_keyboard()
            )
            context.user_data.pop('awaiting_invite_phone', None)
            return
        
        pending_count = crud.count_pending_invites(db, driver.driver_id)
        if pending_count >= 3:
            await update.message.reply_text(
                f"❌ У вас уже {pending_count} активных приглашений в ожидании.\n"
                f"Максимальное количество: 3.\n"
                f"Дождитесь регистрации приглашённых водителей.",
                reply_markup=get_back_keyboard()
            )
            context.user_data.pop('awaiting_invite_phone', None)
            return
        
        referred = crud.get_driver_by_phone(db, target_phone_clean)
        if referred and referred.created_date:
            try:
                if 'T' in referred.created_date:
                    created_date_str = referred.created_date.split('T')[0]
                else:
                    created_date_str = referred.created_date[:10]
                
                created_date = datetime.strptime(created_date_str, '%Y-%m-%d')
                days_ago = (datetime.utcnow() - created_date).days
                
                if days_ago >= 3:
                    await update.message.reply_text(
                        f"❌ Водитель {referred.last_name or 'Без имени'} зарегистрирован {days_ago} дней назад.\n\n"
                        f"Приглашать можно только водителей, зарегистрированных в течение последних 3 дней.",
                        reply_markup=get_back_keyboard()
                    )
                    context.user_data.pop('awaiting_invite_phone', None)
                    return
            except Exception as e:
                logger.error(f"Error parsing created_date: {e}")
        
        referral = crud.create_referral(db, driver.driver_id, target_phone_clean)
        
        if referral:
            remaining_orders = max(0, 100 - (referred.orders_count if referred else 0))
            await update.message.reply_text(
                f"✅ *Приглашение отправлено!*\n\n"
                f"📞 Номер: {target_phone}\n"
                f"🎯 Осталось заказов до награды: {remaining_orders}\n\n"
                f"_Когда водитель выполнит условие, вы получите уведомление_",
                parse_mode='Markdown',
                reply_markup=get_main_keyboard(is_authorized=True)
            )
        else:
            existing = db.query(models.Referral).filter(
                models.Referral.referrer_id == driver.driver_id,
                models.Referral.referred_phone == target_phone_clean
            ).first()
            
            if existing:
                await update.message.reply_text(
                    f"⏳ Вы уже приглашали номер {target_phone}\n"
                    f"Статус: {existing.status}",
                    reply_markup=get_back_keyboard()
                )
            else:
                await update.message.reply_text(
                    f"❌ Ошибка при создании приглашения",
                    reply_markup=get_back_keyboard()
                )
        
        context.user_data.pop('awaiting_invite_phone', None)
        
    except Exception as e:
        logger.error(f"Invite error: {e}")
        await update.message.reply_text(
            f"❌ Ошибка: {str(e)}",
            reply_markup=get_back_keyboard()
        )
        context.user_data.pop('awaiting_invite_phone', None)
    finally:
        db.close()


# ========== ПРИГЛАШЕНИЯ (СПИСОК) ==========

async def my_referrals_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия кнопки 'Мои приглашения'"""
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    
    db = SessionLocal()
    try:
        driver = crud.get_driver_by_telegram_id(db, user.id)
        
        if not driver:
            await query.edit_message_text(
                "❌ Вы не авторизованы.\n\n"
                "Сначала нажмите кнопку «Войти по номеру телефона»",
                reply_markup=get_back_keyboard()
            )
            return
        
        referrals = crud.get_referrals_by_driver(db, driver.driver_id)
        pending_invites = crud.get_pending_invites_by_referrer(db, driver.driver_id)
        
        if not referrals and not pending_invites:
            await query.edit_message_text(
                "📋 *Ваши приглашения*\n\n"
                "У вас пока нет приглашений.\n\n"
                "Нажмите «Пригласить водителя», чтобы начать!",
                parse_mode='Markdown',
                reply_markup=get_back_keyboard()
            )
            return
        
        response = "📋 *Ваши приглашения*\n\n"
        
        for ref in referrals[:10]:
            referred = crud.get_driver(db, ref.referred_id) if ref.referred_id else None
            referred_name = referred.last_name if referred else ref.referred_phone
            
            status_emoji = {
                'pending': '⏳',
                'completed': '✅',
                'rewarded': '🎁'
            }.get(ref.status, '❓')
            
            status_text = {
                'pending': 'ожидает',
                'completed': 'выполнено!',
                'rewarded': 'награда получена'
            }.get(ref.status, ref.status)
            
            response += f"{status_emoji} {referred_name} — {status_text}\n"
        
        for invite in pending_invites:
            days_left = max(0, 7 - (datetime.utcnow() - invite.invited_at).days)
            response += f"⏳ {invite.phone} — ожидает регистрации ({days_left} дн.)\n"
        
        if len(referrals) > 10:
            response += f"\n*... и еще {len(referrals) - 10} приглашений*"
        
        await query.edit_message_text(
            response,
            parse_mode='Markdown',
            reply_markup=get_back_keyboard()
        )
        
    finally:
        db.close()


# ========== СТАТИСТИКА НАГРАД ==========

async def referral_stats_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия кнопки 'Статус наград'"""
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    
    db = SessionLocal()
    try:
        driver = crud.get_driver_by_telegram_id(db, user.id)
        
        if not driver:
            await query.edit_message_text(
                "❌ Вы не авторизованы.\n\n"
                "Сначала нажмите кнопку «Войти по номеру телефона»",
                reply_markup=get_back_keyboard()
            )
            return
        
        stats = crud.get_reward_stats(db, driver.driver_id)
        
        all_referrals = crud.get_referrals_by_driver(db, driver.driver_id)
        completed = sum(1 for r in all_referrals if r.status in ['completed', 'rewarded'])
        pending = sum(1 for r in all_referrals if r.status == 'pending')
        pending_invites = crud.count_pending_invites(db, driver.driver_id)
        
        response = (
            f"💰 *Ваша реферальная статистика*\n\n"
            f"👥 Приглашено водителей: {completed + pending}\n"
            f"✅ Выполнили 100+ заказов: {completed}\n"
            f"⏳ Ожидают выполнения: {pending}\n"
            f"📞 Ожидают регистрации: {pending_invites}\n\n"
            f"🎁 Награда ожидает выдачи: {stats['pending']} бонусов\n"
            f"🏆 Получено наград: {stats['total']} бонусов\n\n"
            f"Приглашайте новых водителей и получайте бонусы!"
        )
        
        await query.edit_message_text(
            response,
            parse_mode='Markdown',
            reply_markup=get_back_keyboard()
        )
        
    finally:
        db.close()


# ========== СТАТИСТИКА ==========

async def stats_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия кнопки 'Статистика'"""
    query = update.callback_query
    await query.answer()
    
    db = SessionLocal()
    try:
        stats = crud.get_driver_statistics(db)
        
        response = (
            f"📈 *Статистика парка*\n\n"
            f"👥 Всего водителей: {stats['total']}\n"
            f"🟢 Работают: {stats['working']}\n"
            f"🟡 Не работают: {stats['not_working']}\n"
            f"🔴 Уволены: {stats['fired']}\n"
            f"✨ Новые (30 дней): {stats['new_last_30_days']}\n"
            f"📊 Среднее заказов: {stats['avg_orders']}\n"
        )
        
        await query.edit_message_text(
            response,
            parse_mode='Markdown',
            reply_markup=get_back_keyboard()
        )
        
    finally:
        db.close()


# ========== ТОП ВОДИТЕЛЕЙ ==========

async def top_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия кнопки 'Топ водителей'"""
    query = update.callback_query
    await query.answer()
    
    db = SessionLocal()
    try:
        drivers = db.query(models.Driver).filter(
            models.Driver.work_status == 'working'
        ).order_by(
            models.Driver.orders_count.desc()
        ).limit(10).all()
        
        if not drivers:
            await query.edit_message_text(
                "Нет данных о водителях",
                reply_markup=get_back_keyboard()
            )
            return
        
        response = "🏆 *Топ-10 водителей по заказам:*\n\n"
        for i, driver in enumerate(drivers, 1):
            response += f"{i}. {driver.last_name or 'Водитель'}\n"
            response += f"   📦 {driver.orders_count} заказов\n\n"
        
        await query.edit_message_text(
            response,
            parse_mode='Markdown',
            reply_markup=get_back_keyboard()
        )
        
    finally:
        db.close()


# ========== ПОИСК ==========

async def search_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия кнопки 'Поиск'"""
    query = update.callback_query
    await query.answer()
    
    context.user_data['awaiting_search'] = True
    
    await query.edit_message_text(
        "🔍 *Поиск водителя*\n\n"
        "Введите фамилию или ID водителя для поиска:",
        parse_mode='Markdown',
        reply_markup=get_back_keyboard()
    )


async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода поискового запроса"""
    if not context.user_data.get('awaiting_search'):
        return
    
    query_text = update.message.text.strip()
    
    db = SessionLocal()
    try:
        drivers = crud.search_drivers(db, query_text)
        
        if not drivers:
            await update.message.reply_text(
                f"❌ Водители по запросу '{query_text}' не найдены",
                reply_markup=get_back_keyboard()
            )
            context.user_data.pop('awaiting_search', None)
            return
        
        response = f"🔍 *Результаты поиска:*\n\n"
        for driver in drivers[:10]:
            response += f"👤 {driver.last_name or 'Без имени'}\n"
            response += f"   🆔 ID: `{driver.driver_id[:12]}...`\n"
            response += f"   📦 Заказов: {driver.orders_count}\n"
            status_emoji = "🟢" if driver.work_status == "working" else "🟡" if driver.work_status == "not_working" else "🔴"
            response += f"   {status_emoji} Статус: {driver.work_status}\n\n"
        
        await update.message.reply_text(
            response[:4096],
            parse_mode='Markdown',
            reply_markup=get_back_keyboard()
        )
        
        context.user_data.pop('awaiting_search', None)
        
    finally:
        db.close()


# ========== ВЫХОД ==========

async def logout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия кнопки 'Выйти'"""
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    
    db = SessionLocal()
    try:
        driver = crud.get_driver_by_telegram_id(db, user.id)
        
        if driver:
            driver.telegram_id = None
            db.commit()
        
        await query.edit_message_text(
            "👋 *До свидания!*\n\n"
            "Вы вышли из аккаунта.\n\n"
            "Чтобы снова пользоваться ботом, нажмите кнопку «Войти по номеру телефона»",
            parse_mode='Markdown',
            reply_markup=get_main_keyboard(is_authorized=False)
        )
        
    finally:
        db.close()


# ========== ПОМОЩЬ ==========

async def help_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия кнопки 'Помощь'"""
    query = update.callback_query
    await query.answer()
    
    help_text = """
❓ *Как пользоваться ботом*

1️⃣ *Вход в систему*
   • Нажмите «Войти по номеру телефона»
   • Введите ваш номер в формате +79001234567

2️⃣ *Приглашение водителей*
   • Нажмите «Пригласить водителя»
   • Введите номер телефона приглашаемого

3️⃣ *Отслеживание прогресса*
   • «Мои приглашения» — статус ваших приглашений
   • «Статус наград» — сколько бонусов вы заработали

4️⃣ *Условия получения награды*
   • Приглашённый водитель должен сделать 100 заказов
   • Награда начисляется автоматически

📞 *Поддержка:* /help_admin (для администраторов)
"""
    
    await query.edit_message_text(
        help_text,
        parse_mode='Markdown',
        reply_markup=get_back_keyboard()
    )


# ========== АДМИНИСТРАТОРСКАЯ СПРАВКА ==========

async def help_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help_admin - полная справка для администратора"""
    if update.effective_user.id not in settings.ADMIN_IDS:
        await update.message.reply_text("⛔ У вас нет прав для этой команды")
        return
    
    help_text = """
📚 *ПОЛНАЯ СПРАВКА ДЛЯ АДМИНИСТРАТОРА*

📊 *Основные команды:*
/stats - Общая статистика по водителям
/top - Топ-10 водителей по заказам
/search <фамилия или ID> - Поиск водителей
/new - Новые водители (последние 30 дней)
/status <working/not_working/fired> - Водители по статусу
/driver <id> - Информация о конкретном водителе
/recent - История обновлений сборщика
/queue - Статистика очереди обновления

🔐 *Реферальная система:*
/auth <телефон> - Привязать Telegram к водителю
/invite <телефон> - Пригласить водителя
/myreferrals - Список моих приглашений
/referralstats - Статистика наград

📞 *Управление данными:*
/export - Экспорт всех водителей в CSV
/update_phones - Обновить номера телефонов
/phonestatus - Статус телефонов в базе

📋 *Примеры использования:*
/search Иванов
/status working
/driver 123456789
/auth +79001234567
/invite +79009876543

🔄 *Автоматические процессы:*
• Сбор данных о водителях: каждые 6 часов
• Обновление телефонов: каждые 6 часов
• Обработка приглашений: каждые 6 часов
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')


# ========== ОСНОВНЫЕ КОМАНДЫ (ТЕКСТОВЫЕ) ==========

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help - справка по реферальной системе"""
    help_text = """
🔐 *Реферальная система приглашений*

📋 *Доступные команды:*

/auth <телефон> - Привязать Telegram к водителю
   Пример: /auth +79001234567

/invite <телефон> - Пригласить водителя
   Пример: /invite +79009876543

/myreferrals - Список моих приглашений
/referralstats - Статистика наград

📊 *Как это работает:*
1. Привяжите свой аккаунт через /auth
2. Приглашайте новых водителей через /invite
3. Когда приглашённый сделает 100 заказов, вы получите награду

📞 *Другие команды:* /stats, /top, /search, /new, /status, /driver, /recent
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')


async def get_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /stats - статистика"""
    await update.message.reply_text("📊 Собираю статистику...")
    
    db = SessionLocal()
    try:
        stats = crud.get_driver_statistics(db)
        
        response = (
            "📈 *Статистика водителей:*\n\n"
            f"👥 *Всего:* {stats['total']}\n"
            f"🟢 *Работают:* {stats['working']}\n"
            f"🟡 *Не работают:* {stats['not_working']}\n"
            f"🔴 *Уволены:* {stats['fired']}\n"
            f"✨ *Новые (30 дней):* {stats['new_last_30_days']}\n"
            f"📊 *Среднее кол-во заказов:* {stats['avg_orders']}\n"
        )
        
        last_log = crud.get_last_collection_log(db)
        if last_log:
            response += f"\n🔄 *Последнее обновление:*\n"
            response += f"   • {last_log.finished_at.strftime('%Y-%m-%d %H:%M')}\n"
            response += f"   • Новых: {last_log.new_drivers_added}\n"
            response += f"   • Обновлено статусов: {last_log.status_updated}\n"
            response += f"   • Обновлено заказов: {last_log.orders_updated}\n"
        
        await update.message.reply_text(response, parse_mode='Markdown')
        
    finally:
        db.close()


async def get_top_drivers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /top - топ водителей по заказам"""
    db = SessionLocal()
    try:
        drivers = db.query(models.Driver).filter(
            models.Driver.work_status == 'working'
        ).order_by(
            models.Driver.orders_count.desc()
        ).limit(10).all()
        
        if not drivers:
            await update.message.reply_text("Нет данных о водителях")
            return
        
        response = "🏆 *Топ-10 водителей по заказам:*\n\n"
        for i, driver in enumerate(drivers, 1):
            response += f"{i}. {driver.last_name or 'Водитель'}\n"
            response += f"   📦 Заказов: {driver.orders_count}\n"
            response += "\n"
        
        await update.message.reply_text(response[:4096], parse_mode='Markdown')
        
    finally:
        db.close()


async def search_driver(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /search <фамилия или ID>"""
    if not context.args:
        await update.message.reply_text(
            "❓ Укажите фамилию или ID водителя\n"
            "Пример: /search Иванов"
        )
        return
    
    query_text = ' '.join(context.args)
    await update.message.reply_text(f"🔍 Ищу: {query_text}...")
    
    db = SessionLocal()
    try:
        drivers = crud.search_drivers(db, query_text)
        
        if not drivers:
            await update.message.reply_text(f"Водители по запросу '{query_text}' не найдены")
            return
        
        response = f"🔍 *Результаты поиска:*\n\n"
        for driver in drivers[:10]:
            response += f"👤 {driver.last_name or 'Без имени'}\n"
            response += f"   🆔 ID: `{driver.driver_id[:12]}...`\n"
            response += f"   📦 Заказов: {driver.orders_count}\n"
            status_emoji = "🟢" if driver.work_status == "working" else "🟡" if driver.work_status == "not_working" else "🔴"
            response += f"   {status_emoji} Статус: {driver.work_status}\n\n"
        
        await update.message.reply_text(response[:4096], parse_mode='Markdown')
        
    finally:
        db.close()


async def get_new_drivers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /new - новые водители"""
    db = SessionLocal()
    try:
        cutoff = datetime.utcnow() - timedelta(days=30)
        
        drivers = db.query(models.Driver).filter(
            models.Driver.created_at >= cutoff
        ).order_by(
            models.Driver.created_at.desc()
        ).limit(20).all()
        
        if not drivers:
            await update.message.reply_text("Нет новых водителей за последние 30 дней")
            return
        
        response = "✨ *Новые водители (последние 30 дней):*\n\n"
        for driver in drivers:
            response += f"👤 {driver.last_name or 'Без имени'}\n"
            response += f"   📅 Добавлен: {driver.created_at.strftime('%Y-%m-%d')}\n"
            response += f"   📦 Заказов: {driver.orders_count}\n\n"
        
        await update.message.reply_text(response[:4096], parse_mode='Markdown')
        
    finally:
        db.close()


async def get_drivers_by_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /status <working/not_working/fired>"""
    if not context.args:
        await update.message.reply_text(
            "❓ Укажите статус\n"
            "Варианты: working, not_working, fired\n"
            "Пример: /status working"
        )
        return
    
    status = context.args[0].lower()
    if status not in ['working', 'not_working', 'fired']:
        await update.message.reply_text("Неверный статус. Используйте: working, not_working или fired")
        return
    
    db = SessionLocal()
    try:
        drivers = crud.get_drivers_by_status(db, status)
        
        status_emoji = "🟢" if status == "working" else "🟡" if status == "not_working" else "🔴"
        response = f"{status_emoji} *Водители со статусом '{status}':*\n\n"
        
        for driver in drivers[:20]:
            response += f"👤 {driver.last_name or 'Без имени'}\n"
            response += f"   📦 Заказов: {driver.orders_count}\n"
            if driver.last_updated:
                days_ago = (datetime.utcnow() - driver.last_updated).days
                response += f"   🕒 Обновлен: {days_ago} дней назад\n"
            response += "\n"
        
        if len(drivers) > 20:
            response += f"\n*... и еще {len(drivers) - 20} водителей*"
        
        await update.message.reply_text(response[:4096], parse_mode='Markdown')
        
    finally:
        db.close()


async def get_driver_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /driver <id> - информация о водителе"""
    if not context.args:
        await update.message.reply_text(
            "❓ Укажите ID водителя\n"
            "Пример: /driver 123456789"
        )
        return
    
    driver_id = context.args[0]
    db = SessionLocal()
    try:
        driver = crud.get_driver(db, driver_id)
        
        if not driver:
            await update.message.reply_text(f"Водитель с ID {driver_id} не найден")
            return
        
        status_emoji = "🟢" if driver.work_status == "working" else "🟡" if driver.work_status == "not_working" else "🔴"
        
        response = (
            f"👤 *Информация о водителе:*\n\n"
            f"*Фамилия:* {driver.last_name or 'Не указана'}\n"
            f"*ID:* `{driver.driver_id}`\n"
            f"*Статус:* {status_emoji} {driver.work_status}\n"
            f"*Заказов:* 📦 {driver.orders_count}\n"
            f"*Баланс:* 💰 {driver.balance} {driver.currency}\n"
            f"*Текущий статус:* {driver.current_status}\n"
        )
        
        if driver.created_date:
            response += f"*Дата регистрации:* {driver.created_date[:10]}\n"
        
        if driver.last_transaction_date:
            response += f"*Последняя транзакция:* {driver.last_transaction_date[:10]}\n"
        
        if driver.last_updated:
            days_ago = (datetime.utcnow() - driver.last_updated).days
            response += f"*Последнее обновление:* {days_ago} дней назад\n"
        
        await update.message.reply_text(response, parse_mode='Markdown')
        
    finally:
        db.close()


async def get_recent_updates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /recent - последние обновления"""
    db = SessionLocal()
    try:
        logs = crud.get_collection_history(db, limit=5)
        
        if not logs:
            await update.message.reply_text("Нет данных об обновлениях")
            return
        
        response = "🔄 *Последние обновления:*\n\n"
        for log in logs:
            status_emoji = "✅" if log.status == "success" else "❌"
            response += f"{status_emoji} *{log.finished_at.strftime('%Y-%m-%d %H:%M')}*\n"
            if log.status == "success":
                response += f"   ✨ Новых: {log.new_drivers_added}\n"
                response += f"   🔄 Обновлено статусов: {log.status_updated}\n"
                response += f"   📊 Обновлено заказов: {log.orders_updated}\n"
            else:
                response += f"   ⚠️ Ошибка: {log.error_message}\n"
            response += "\n"
        
        await update.message.reply_text(response, parse_mode='Markdown')
        
    finally:
        db.close()


async def queue_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /queue - статистика очереди обновления"""
    if update.effective_user.id not in settings.ADMIN_IDS:
        await update.message.reply_text("⛔ У вас нет прав для этой команды")
        return
    
    db = SessionLocal()
    try:
        stats = crud.get_queue_stats(db)
        
        response = (
            "📋 *Статистика очереди обновления:*\n\n"
            f"👥 Всего в очереди: {stats['total']}\n"
            f"⭐ Высокий приоритет (новые): {stats['high_priority']}\n"
            f"📊 Обычный приоритет: {stats['low_priority']}\n"
            f"⏱️ Среднее время ожидания: {stats['avg_wait_hours']} часов\n\n"
            f"Обновление происходит по принципу FIFO:\n"
            f"сначала обновляются водители, которые дольше всех ждали"
        )
        
        await update.message.reply_text(response, parse_mode='Markdown')
        
    finally:
        db.close()


# ========== ЭКСПОРТ ==========

async def export_drivers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Экспорт всех водителей в CSV файл с реферальной статистикой"""
    if update.effective_user.id not in settings.ADMIN_IDS:
        await update.message.reply_text("⛔ У вас нет прав для этой команды")
        return
    
    await update.message.reply_text("📊 Начинаю экспорт данных с реферальной статистикой...\n⏳ Это может занять некоторое время")
    
    db = SessionLocal()
    
    try:
        total = db.query(models.Driver).count()
        
        if total == 0:
            await update.message.reply_text("Нет данных для экспорта")
            return
        
        MAX_ROWS = 15000
        if total > MAX_ROWS:
            await update.message.reply_text(
                f"⚠️ В базе {total} водителей, что превышает лимит экспорта ({MAX_ROWS}).\n"
                f"Будут экспортированы первые {MAX_ROWS} водителей.",
                reply_markup=get_back_keyboard()
            )
            total = MAX_ROWS
        
        output = io.StringIO()
        writer = csv.writer(output, delimiter=';')
        
        writer.writerow([
            'ID', 'Фамилия', 'Статус', 'Заказы', 
            'Баланс', 'Валюта', 'Текущий статус', 
            'Дата регистрации', 'Последняя транзакция', 
            'Последнее обновление', 'Дата добавления', 'Телефон',
            'Количество приглашений', 'Приглашённые (ID)', 'Выполнено 100+ заказов',
            'Количество наград', 'Сумма наград (бонусы)', 'Кто пригласил (ID)'
        ])
        
        batch_size = 500
        offset = 0
        last_progress = 0
        
        while offset < total:
            drivers = db.query(models.Driver).order_by(
                models.Driver.driver_id
            ).offset(offset).limit(min(batch_size, total - offset)).all()
            
            driver_ids = [d.driver_id for d in drivers]
            
            referrals = db.query(models.Referral).filter(
                models.Referral.referrer_id.in_(driver_ids)
            ).all()
            
            referral_stats = {}
            for r in referrals:
                if r.referrer_id not in referral_stats:
                    referral_stats[r.referrer_id] = {
                        'count': 0,
                        'completed': 0,
                        'referred_ids': []
                    }
                referral_stats[r.referrer_id]['count'] += 1
                referral_stats[r.referrer_id]['referred_ids'].append(r.referred_phone)
                if r.status in ['completed', 'rewarded']:
                    referral_stats[r.referrer_id]['completed'] += 1
            
            referrers = db.query(models.Referral).filter(
                models.Referral.referred_phone.in_([d.phone for d in drivers if d.phone])
            ).all()
            referrer_map = {r.referred_phone: r.referrer_id for r in referrers}
            
            rewards = db.query(models.ReferralReward).filter(
                models.ReferralReward.driver_id.in_(driver_ids)
            ).all()
            reward_stats = {}
            for rw in rewards:
                if rw.driver_id not in reward_stats:
                    reward_stats[rw.driver_id] = {'count': 0, 'amount': 0}
                reward_stats[rw.driver_id]['count'] += 1
                if rw.status == 'paid':
                    reward_stats[rw.driver_id]['amount'] += rw.amount
            
            for driver in drivers:
                ref = referral_stats.get(driver.driver_id, {'count': 0, 'completed': 0, 'referred_ids': []})
                referred_ids_str = ', '.join(ref['referred_ids'][:3])
                if len(ref['referred_ids']) > 3:
                    referred_ids_str += f"... (+{len(ref['referred_ids']) - 3})"
                
                rew = reward_stats.get(driver.driver_id, {'count': 0, 'amount': 0})
                
                writer.writerow([
                    driver.driver_id,
                    driver.last_name or '',
                    driver.work_status,
                    driver.orders_count,
                    driver.balance,
                    driver.currency,
                    driver.current_status,
                    driver.created_date[:10] if driver.created_date else '',
                    driver.last_transaction_date[:10] if driver.last_transaction_date else '',
                    driver.last_updated.strftime('%Y-%m-%d %H:%M') if driver.last_updated else '',
                    driver.created_at.strftime('%Y-%m-%d %H:%M') if driver.created_at else '',
                    driver.phone if driver.phone else '',
                    ref['count'],
                    referred_ids_str,
                    ref['completed'],
                    rew['count'],
                    rew['amount'],
                    referrer_map.get(driver.phone, '') if driver.phone else ''
                ])
            
            offset += batch_size
            
            progress = int(offset / total * 100)
            if progress >= last_progress + 10:
                last_progress = progress
                await update.message.reply_text(f"📊 Экспортировано {min(offset, total)}/{total} записей ({progress}%)...")
        
        output_bytes = io.BytesIO()
        output_bytes.write(output.getvalue().encode('utf-8-sig'))
        output_bytes.seek(0)
        
        await update.message.reply_document(
            document=output_bytes,
            filename=f'drivers_export_{datetime.now().strftime("%Y%m%d_%H%M")}.csv',
            caption=f'📊 Экспорт данных о водителях с реферальной статистикой\n'
                    f'Всего записей: {min(total, MAX_ROWS)}\n'
                    f'Дата: {datetime.now().strftime("%Y-%m-%d %H:%M")}'
        )
        
    except Exception as e:
        logger.error(f"Export error: {e}")
        await update.message.reply_text(f"❌ Ошибка при экспорте: {str(e)}")
    finally:
        db.close()


# ========== ОБНОВЛЕНИЕ ТЕЛЕФОНОВ ==========

async def update_phones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /update_phones [days] - обновление телефонов (админ)"""
    if update.effective_user.id not in settings.ADMIN_IDS:
        await update.message.reply_text("⛔ У вас нет прав для этой команды")
        return
    
    days = 30
    if context.args and context.args[0].isdigit():
        days = int(context.args[0])
    
    await update.message.reply_text(f"📞 Начинаю обновление номеров телефонов (за последние {days} дней)...\n⚠️ Это может занять несколько минут")
    
    async def run():
        try:
            result = await asyncio.to_thread(
                DataCollector().update_all_driver_phones,
                batch_size=500,
                days_stale=days
            )
            await update.message.reply_text(
                f"✅ Обновление завершено!\n\n"
                f"📞 Обновлено: {result['updated']}\n"
                f"❌ Ошибок: {len(result['errors'])}\n\n"
                f"Следующее автоматическое обновление через 6 часов",
                parse_mode='Markdown'
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка: {str(e)}")
    
    asyncio.create_task(run())


async def phone_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /phonestatus - статус телефонов в базе"""
    if update.effective_user.id not in settings.ADMIN_IDS:
        await update.message.reply_text("⛔ У вас нет прав для этой команды")
        return
    
    db = SessionLocal()
    try:
        total = db.query(models.Driver).count()
        with_phone = db.query(models.Driver).filter(
            models.Driver.phone.isnot(None),
            models.Driver.phone != ''
        ).count()
        without_phone = total - with_phone
        
        last = db.query(models.Driver).filter(
            models.Driver.phone_updated_at.isnot(None)
        ).order_by(models.Driver.phone_updated_at.desc()).first()
        
        last_time = last.phone_updated_at.strftime('%Y-%m-%d %H:%M') if last and last.phone_updated_at else 'никогда'
        
        await update.message.reply_text(
            f"📊 *Статус телефонов водителей:*\n"
            f"👥 Всего: {total}\n"
            f"📞 С телефонами: {with_phone}\n"
            f"❌ Без телефонов: {without_phone}\n"
            f"📈 Прогресс: {round(with_phone/total*100, 1)}%\n"
            f"🕐 Последнее обновление: {last_time}\n\n"
            f"Автоматическое обновление: каждые 6 часов",
            parse_mode='Markdown'
        )
    finally:
        db.close()


# ========== ОБРАБОТКА НЕИЗВЕСТНЫХ КОМАНД ==========

async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка неизвестных команд"""
    await update.message.reply_text(
        "❌ Неизвестная команда.\n"
        "Используйте /help для списка доступных команд\n\n"
        "Или нажмите /start для открытия меню"
    )


# ========== ОБРАБОТЧИК КНОПОК ==========

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка всех нажатий на кнопки"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "menu":
        await menu(update, context)
    elif query.data == "auth":
        await auth_button(update, context)
    elif query.data == "register":
        await auth_button(update, context)
    elif query.data == "consent_yes":
        await handle_consent(update, context)
    elif query.data == "consent_no":
        await handle_consent(update, context)
    elif query.data == "revoke_confirm":
        await handle_revoke_consent(update, context)
    elif query.data == "revoke_cancel":
        await handle_revoke_consent(update, context)
    elif query.data == "invite":
        await invite_button(update, context)
    elif query.data == "my_referrals":
        await my_referrals_button(update, context)
    elif query.data == "referral_stats":
        await referral_stats_button(update, context)
    elif query.data == "stats":
        await stats_button(update, context)
    elif query.data == "top":
        await top_button(update, context)
    elif query.data == "search":
        await search_button(update, context)
    elif query.data == "help":
        await help_button(update, context)
    elif query.data == "logout":
        await logout(update, context)


# ========== ОТЗЫВ СОГЛАСИЯ ==========

async def revoke_consent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /revoke_consent - отзыв согласия на обработку данных"""
    user = update.effective_user
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Да, подтверждаю отзыв", callback_data="revoke_confirm"),
            InlineKeyboardButton("❌ Нет, оставить согласие", callback_data="revoke_cancel")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "⚠️ *Отзыв согласия на обработку данных*\n\n"
        "Вы уверены, что хотите отозвать согласие?\n\n"
        "После отзыва:\n"
        "• Ваш аккаунт будет отвязан от бота\n"
        "• Ваши реферальные приглашения станут неактивными\n"
        "• Вы не сможете получать новые награды\n\n"
        "Вы можете снова дать согласие через /start в любой момент",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )


async def handle_revoke_consent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка подтверждения отзыва согласия"""
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    
    if query.data == "revoke_confirm":
        db = SessionLocal()
        try:
            crud.update_user_consent(db, user.id, False)
            
            driver = crud.get_driver_by_telegram_id(db, user.id)
            if driver:
                driver.telegram_id = None
                db.commit()
            
            await query.edit_message_text(
                "✅ *Согласие отозвано.*\n\n"
                "Ваши данные были отвязаны от бота.\n\n"
                "Если вы захотите снова пользоваться ботом, нажмите /start и дайте новое согласие.",
                parse_mode='Markdown'
            )
        finally:
            db.close()
    else:
        await query.edit_message_text(
            "Операция отменена. Ваше согласие на обработку данных остаётся в силе.",
            parse_mode='Markdown'
        )