import io
import csv
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime, timedelta
import json
import logging

from database.session import SessionLocal
from database import crud, models
from shared.config import settings
from collector.main import DataCollector

logger = logging.getLogger(__name__)


# ========== ОСНОВНЫЕ КОМАНДЫ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    user = update.effective_user
    
    # Сохраняем пользователя
    db = SessionLocal()
    try:
        crud.add_or_update_user(
            db,
            telegram_id=user.id,
            username=user.username,
            is_admin=user.id in settings.ADMIN_IDS
        )
    finally:
        db.close()
    
    keyboard = []
    if update.effective_user.id in settings.ADMIN_IDS:
        keyboard.append([InlineKeyboardButton("📊 Экспорт данных", callback_data="export")])
        keyboard.append([InlineKeyboardButton("📞 Обновить телефоны", callback_data="update_phones")])
    
    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    
    await update.message.reply_text(
        f"👋 Привет, {user.first_name}!\n\n"
        f"Я бот для доступа к данным водителей Яндекс.Такси.\n\n"
        f"📋 *Доступные команды:*\n"
        f"/help - подробная справка\n\n"
        f"🔐 *Реферальная система:*\n"
        f"/auth <телефон> - привязать Telegram к водителю\n"
        f"/invite <телефон> - пригласить водителя\n"
        f"/my_referrals - мои приглашения\n"
        f"/referral_stats - статистика наград",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help"""
    help_text = """
📚 *Подробная справка по командам:*

🔐 *Реферальная система:*
/auth <телефон> - Привязать Telegram к водителю
/invite <телефон> - Пригласить водителя
/my_referrals - Список моих приглашений
/referral_stats - Статистика наград

*Примеры использования:*
/search Иванов
/status working
/driver 123456789
/auth +79001234567
/invite +79009876543
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def help_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help_admin - полная справка для администратора (только для ADMIN_IDS)"""
    # Проверка по ADMIN_IDS из переменных окружения
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

📊 *Статистика системы:* /stats, /queue, /phonestatus
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')

# ========== РЕФЕРАЛЬНАЯ СИСТЕМА ==========

async def auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /auth - привязка Telegram аккаунта к водителю по номеру телефона"""
    user = update.effective_user
    
    if not context.args:
        await update.message.reply_text(
            "❓ Укажите номер телефона для авторизации.\n"
            "Пример: /auth +79001234567\n\n"
            "Номер должен совпадать с тем, который указан в вашем профиле Яндекс.Такси"
        )
        return
    
    phone = context.args[0]
    phone_clean = phone.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
    
    db = SessionLocal()
    try:
        driver = crud.get_driver_by_phone(db, phone_clean)
        
        if not driver:
            await update.message.reply_text(
                f"❌ Водитель с номером {phone} не найден в базе данных.\n\n"
                f"Убедитесь, что:\n"
                f"1. Номер указан в формате +79001234567\n"
                f"2. Ваш номер телефона есть в базе (он обновляется раз в сутки)"
            )
            return
        
        existing = crud.get_driver_by_telegram_id(db, user.id)
        
        if existing and existing.driver_id != driver.driver_id:
            await update.message.reply_text(
                f"⚠️ Ваш Telegram аккаунт уже привязан к другому водителю.\n"
                f"Свяжитесь с администратором для смены привязки."
            )
            return
        
        driver.telegram_id = user.id
        db.commit()
        
        await update.message.reply_text(
            f"✅ Аккаунт привязан!\n\n"
            f"👤 Водитель: {driver.last_name or 'Без имени'}\n"
            f"📞 Телефон: {driver.phone or 'Не указан'}\n"
            f"📦 Заказов: {driver.orders_count}\n\n"
            f"📋 *Доступные команды:*\n"
            f"/invite — пригласить водителя\n"
            f"/my_referrals — мои приглашения\n"
            f"/referral_stats — статистика наград",
            parse_mode='Markdown'
        )
        
    finally:
        db.close()


async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /whoami - информация о привязанном аккаунте"""
    user = update.effective_user
    
    db = SessionLocal()
    try:
        driver = crud.get_driver_by_telegram_id(db, user.id)
        
        if not driver:
            await update.message.reply_text(
                "❌ Вы не авторизованы.\n"
                "Используйте /auth +79001234567 для привязки аккаунта"
            )
            return
        
        await update.message.reply_text(
            f"👤 *Ваш профиль*\n\n"
            f"Фамилия: {driver.last_name or 'Не указана'}\n"
            f"Телефон: {driver.phone or 'Не указан'}\n"
            f"Заказов: {driver.orders_count}\n"
            f"Статус: {driver.work_status}\n\n"
            f"📋 /invite — пригласить водителя\n"
            f"📋 /my_referrals — мои приглашения",
            parse_mode='Markdown'
        )
        
    finally:
        db.close()


async def invite_driver(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /invite <телефон> - пригласить водителя по номеру телефона"""
    user = update.effective_user
    
    db = SessionLocal()
    try:
        # Проверяем авторизацию
        driver = crud.get_driver_by_telegram_id(db, user.id)
        
        if not driver:
            await update.message.reply_text(
                "❌ Вы не авторизованы.\n"
                "Используйте /auth +79001234567 для привязки аккаунта"
            )
            return
        
        if not context.args:
            await update.message.reply_text(
                "❓ Укажите номер телефона водителя, которого хотите пригласить.\n"
                "Пример: /invite +79001234567"
            )
            return
        
        target_phone = context.args[0]
        target_phone_clean = target_phone.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
        
        # Ищем приглашаемого водителя по телефону
        referred = crud.get_driver_by_phone(db, target_phone_clean)
        
        if not referred:
            # Проверяем, есть ли уже приглашение в ожидающих
            pending = crud.get_pending_invite_by_phone(db, driver.driver_id, target_phone_clean)
            if pending:
                await update.message.reply_text(
                    f"⏳ Приглашение для номера {target_phone} уже отправлено и ожидает регистрации водителя.\n"
                    f"Дата отправки: {pending.invited_at.strftime('%Y-%m-%d %H:%M')}"
                )
                return
            
            # Проверяем лимит ожидающих приглашений (не более 3)
            pending_count = crud.count_pending_invites(db, driver.driver_id)
            if pending_count >= 3:
                await update.message.reply_text(
                    f"❌ У вас уже {pending_count} активных приглашений в ожидании.\n"
                    f"Максимальное количество одновременных приглашений: 3.\n"
                    f"Дождитесь регистрации приглашённых водителей или отмены старых приглашений."
                )
                return
            
            # Создаём ожидающее приглашение
            pending_invite = crud.create_pending_invite(db, driver.driver_id, target_phone_clean)
            
            await update.message.reply_text(
                f"📞 Приглашение для номера {target_phone} отправлено!\n\n"
                f"⏳ Статус: ожидает регистрации водителя\n"
                f"📅 Дата отправки: {pending_invite.invited_at.strftime('%Y-%m-%d %H:%M')}\n"
                f"⏰ Приглашение будет активно 7 дней.\n\n"
                f"_Если водитель зарегистрируется в течение недели, приглашение будет автоматически подтверждено._",
                parse_mode='Markdown'
            )
            return
        
        # ========== ПРОВЕРКА 1: ДАТА РЕГИСТРАЦИИ (не ранее 3 дней) ==========
        if referred.created_date:
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
                        f"Приглашать можно только водителей, зарегистрированных в течение последних 3 дней.\n"
                        f"Дата регистрации: {created_date_str}"
                    )
                    return
            except Exception as e:
                logger.error(f"Error parsing created_date: {e}")
                await update.message.reply_text(
                    f"❌ Не удалось определить дату регистрации водителя.\n"
                    f"Попробуйте позже."
                )
                return
        else:
            await update.message.reply_text(
                f"❌ Не удалось определить дату регистрации водителя {referred.last_name or 'Без имени'}.\n\n"
                f"Приглашать можно только водителей, зарегистрированных в течение последних 3 дней."
            )
            return
        
        # ========== ПРОВЕРКА 2: НЕЛЬЗЯ ПРИГЛАСИТЬ СЕБЯ ==========
        if referred.driver_id == driver.driver_id:
            await update.message.reply_text("❌ Нельзя пригласить самого себя")
            return
        
        # ========== ПРОВЕРКА 3: ЗАПРЕТ ВЗАИМНЫХ ПРИГЛАШЕНИЙ ==========
        # Проверяем, не приглашал ли уже этот водитель текущего
        reverse_referral = crud.get_referrer_by_driver(db, driver.driver_id)
        
        if reverse_referral and reverse_referral.referrer_id == referred.driver_id:
            await update.message.reply_text(
                f"❌ Невозможно пригласить этого водителя.\n\n"
                f"Водитель {referred.last_name or 'Без имени'} уже пригласил вас.\n"
                f"Взаимные приглашения запрещены."
            )
            return
        
        # ========== ПРОВЕРКА 4: СУЩЕСТВУЮЩЕЕ ПРИГЛАШЕНИЕ ==========
        existing = crud.get_referrals_by_driver(db, driver.driver_id)
        existing_for_referred = any(r.referred_id == referred.driver_id for r in existing)
        
        if existing_for_referred:
            await update.message.reply_text(f"❌ Вы уже приглашали этого водителя.")
            return
        
        # ========== СОЗДАНИЕ ПРИГЛАШЕНИЯ ==========
        referral = crud.create_referral(db, driver.driver_id, referred.driver_id)
        
        if referral:
            remaining_orders = max(0, 100 - referred.orders_count)
            await update.message.reply_text(
                f"✅ Приглашение отправлено!\n\n"
                f"👤 Водитель: {referred.last_name or 'Без имени'}\n"
                f"📞 Телефон: {referred.phone or 'Не указан'}\n"
                f"📦 Текущее количество заказов: {referred.orders_count}\n\n"
                f"🎯 Осталось заказов до награды: {remaining_orders}\n"
                f"💰 Награда: 100 бонусов"
            )
        else:
            await update.message.reply_text("❌ Ошибка при создании приглашения")
            
    finally:
        db.close()


async def my_referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /my_referrals - список моих приглашений"""
    user = update.effective_user
    
    db = SessionLocal()
    try:
        driver = crud.get_driver_by_telegram_id(db, user.id)
        
        if not driver:
            await update.message.reply_text(
                "❌ Вы не авторизованы.\n"
                "Используйте /auth +79001234567 для привязки аккаунта"
            )
            return
        
        # Получаем обычные приглашения
        referrals = crud.get_referrals_by_driver(db, driver.driver_id)
        
        # Получаем ожидающие приглашения
        pending_invites = crud.get_pending_invites_by_referrer(db, driver.driver_id)
        
        if not referrals and not pending_invites:
            await update.message.reply_text(
                "📋 У вас пока нет приглашений.\n\n"
                "Используйте /invite +79001234567, чтобы пригласить водителя"
            )
            return
        
        response = "📋 *Ваши приглашения:*\n\n"
        
        # Обычные приглашения
        for ref in referrals[:20]:
            referred = crud.get_driver(db, ref.referred_id)
            referred_name = referred.last_name[:12] + "..." if referred and len(referred.last_name or '') > 15 else (referred.last_name if referred else "Водитель")
            
            status_emoji = {
                'pending': '⏳',
                'completed': '✅',
                'rewarded': '🎁'
            }.get(ref.status, '❓')
            
            status_text = {
                'pending': 'ожидает',
                'completed': 'выполнено (награда готова)',
                'rewarded': 'награда получена'
            }.get(ref.status, ref.status)
            
            response += f"{status_emoji} {referred_name} — {status_text}\n"
        
        # Ожидающие приглашения
        for invite in pending_invites:
            days_left = max(0, 7 - (datetime.utcnow() - invite.invited_at).days)
            phone_display = invite.phone[:10] if len(invite.phone) > 10 else invite.phone
            response += f"⏳ {phone_display}*** — ожидает регистрации ({days_left} дн.)\n"
        
        if len(referrals) > 20:
            response += f"\n*... и еще {len(referrals) - 20} приглашений*"
        
        await update.message.reply_text(response, parse_mode='Markdown')
        
    finally:
        db.close()


async def referral_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /referral_stats - статистика наград"""
    user = update.effective_user
    
    db = SessionLocal()
    try:
        driver = crud.get_driver_by_telegram_id(db, user.id)
        
        if not driver:
            await update.message.reply_text(
                "❌ Вы не авторизованы.\n"
                "Используйте /auth +79001234567 для привязки аккаунта"
            )
            return
        
        stats = crud.get_reward_stats(db, driver.driver_id)
        
        # Получаем все приглашения через crud
        all_referrals = crud.get_referrals_by_driver(db, driver.driver_id)
        completed = sum(1 for r in all_referrals if r.status in ['completed', 'rewarded'])
        pending = sum(1 for r in all_referrals if r.status == 'pending')
        
        pending_invites = crud.count_pending_invites(db, driver.driver_id)
        
        await update.message.reply_text(
            f"💰 *Реферальная статистика*\n\n"
            f"👥 Приглашено водителей: {completed + pending}\n"
            f"✅ Завершено (100 заказов): {completed}\n"
            f"⏳ Ожидают выполнения: {pending}\n"
            f"📞 Ожидают регистрации: {pending_invites}\n\n"
            f"🎁 Ожидает награда: {stats['pending']} бонусов\n"
            f"🏆 Получено наград: {stats['total']} бонусов\n\n"
            f"_Приглашайте водителей и получайте бонусы!_",
            parse_mode='Markdown'
        )
        
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
                f"_Следующее автоматическое обновление через 6 часов_",
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
            f"_Автоматическое обновление: каждые 6 часов_",
            parse_mode='Markdown'
        )
    finally:
        db.close()


# ========== ОСНОВНЫЕ КОМАНДЫ (СТАТИСТИКА, ПОИСК И Т.Д.) ==========

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
    
    query = ' '.join(context.args)
    await update.message.reply_text(f"🔍 Ищу: {query}...")
    
    db = SessionLocal()
    try:
        drivers = crud.search_drivers(db, query)
        
        if not drivers:
            await update.message.reply_text(f"Водители по запросу '{query}' не найдены")
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
            f"_Обновление происходит по принципу FIFO:_\n"
            f"_сначала обновляются водители, которые дольше всех ждали_"
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
        # Получаем общее количество водителей
        total = db.query(models.Driver).count()
        
        if total == 0:
            await update.message.reply_text("Нет данных для экспорта")
            return
        
        # Создаём CSV в памяти
        output = io.StringIO()
        writer = csv.writer(output, delimiter=';')
        
        # Расширенные заголовки с реферальной информацией
        writer.writerow([
            'ID', 'Фамилия', 'Статус', 'Заказы', 
            'Баланс', 'Валюта', 'Текущий статус', 
            'Дата регистрации', 'Последняя транзакция', 
            'Последнее обновление', 'Дата добавления', 'Телефон',
            'Количество приглашений', 'Приглашённые (ID)', 'Выполнено 100+ заказов',
            'Количество наград', 'Сумма наград (бонусы)', 'Кто пригласил (ID)'
        ])
        
        # Экспортируем пачками по 500 записей
        batch_size = 500
        offset = 0
        last_progress = 0
        
        while offset < total:
            # Получаем пачку водителей
            drivers = db.query(models.Driver).order_by(
                models.Driver.driver_id
            ).offset(offset).limit(batch_size).all()
            
            # Собираем все ID водителей для массовой загрузки реферальных данных
            driver_ids = [d.driver_id for d in drivers]
            
            # Массово получаем реферальные данные для всех водителей в пачке
            # Кого пригласил этот водитель
            referrals = db.query(models.Referral).filter(
                models.Referral.referrer_id.in_(driver_ids)
            ).all()
            
            # Статистика по приглашениям (количество, выполненные, ID приглашённых)
            referral_stats = {}
            for r in referrals:
                if r.referrer_id not in referral_stats:
                    referral_stats[r.referrer_id] = {
                        'count': 0,
                        'completed': 0,
                        'referred_ids': []
                    }
                referral_stats[r.referrer_id]['count'] += 1
                referral_stats[r.referrer_id]['referred_ids'].append(r.referred_id)
                if r.status in ['completed', 'rewarded']:
                    referral_stats[r.referrer_id]['completed'] += 1
            
            # Кто пригласил этого водителя
            referrers = db.query(models.Referral).filter(
                models.Referral.referred_id.in_(driver_ids)
            ).all()
            referrer_map = {r.referred_id: r.referrer_id for r in referrers}
            
            # Награды для водителей
            rewards = db.query(models.ReferralReward).filter(
                models.ReferralReward.driver_id.in_(driver_ids)
            ).all()
            reward_stats = {}
            for rw in rewards:
                if rw.driver_id not in reward_stats:
                    reward_stats[rw.driver_id] = {
                        'count': 0,
                        'amount': 0
                    }
                reward_stats[rw.driver_id]['count'] += 1
                if rw.status == 'paid':
                    reward_stats[rw.driver_id]['amount'] += rw.amount
            
            for driver in drivers:
                # Реферальная статистика
                ref = referral_stats.get(driver.driver_id, {'count': 0, 'completed': 0, 'referred_ids': []})
                referred_ids_str = ', '.join(ref['referred_ids'][:5])  # Ограничиваем 5 ID
                if len(ref['referred_ids']) > 5:
                    referred_ids_str += f"... +{len(ref['referred_ids']) - 5}"
                
                # Награды
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
                    referrer_map.get(driver.driver_id, '')
                ])
            
            offset += batch_size
            
            # Отправляем статус каждые 2000 записей
            progress = int(offset / total * 100)
            if progress >= last_progress + 10:
                last_progress = progress
                await update.message.reply_text(f"📊 Экспортировано {offset}/{total} записей ({progress}%)...")
        
        # Конвертируем в bytes для отправки
        output_bytes = io.BytesIO()
        output_bytes.write(output.getvalue().encode('utf-8-sig'))
        output_bytes.seek(0)
        
        # Отправляем файл
        await update.message.reply_document(
            document=output_bytes,
            filename=f'drivers_export_{datetime.now().strftime("%Y%m%d_%H%M")}.csv',
            caption=f'📊 Экспорт данных о водителях с реферальной статистикой\n'
                    f'Всего записей: {total}\n'
                    f'Дата: {datetime.now().strftime("%Y-%m-%d %H:%M")}\n\n'
                    f'📋 *Описание колонок:*\n'
                    f'• "Количество приглашений" — сколько водителей пригласил\n'
                    f'• "Выполнено 100+ заказов" — сколько приглашённых сделали 100+ заказов\n'
                    f'• "Количество наград" — сколько наград получено\n'
                    f'• "Сумма наград" — общее количество бонусов',
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Export error: {e}")
        await update.message.reply_text(f"❌ Ошибка при экспорте: {str(e)}")
    finally:
        db.close()


# ========== КНОПКИ ==========

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий на кнопки"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "export":
        await export_drivers(query.message, context)
    elif query.data == "update_phones":
        await update_phones(query.message, context)


# ========== ОБРАБОТКА НЕИЗВЕСТНЫХ КОМАНД ==========

async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка неизвестных команд"""
    await update.message.reply_text(
        "❌ Неизвестная команда.\n"
        "Используйте /help для списка доступных команд"
    )