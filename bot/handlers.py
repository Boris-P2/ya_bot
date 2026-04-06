import io
import csv
from telegram import Update
from telegram.ext import ContextTypes
from datetime import datetime, timedelta
import json
import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from database.session import SessionLocal
from database import crud
from database import models  # <-- ДОБАВЛЯЕМ ЭТОТ ИМПОРТ
from shared.config import settings
from collector.main import DataCollector  # ← ДОБАВИТЬ ЭТУ СТРОКУ

logger = logging.getLogger(__name__)

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
    
    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    
    
    await update.message.reply_text(
        f"👋 Привет, {user.first_name}!\n\n"
        f"Я бот для доступа к данным водителей Яндекс.Такси.\n\n"
        f"📋 *Доступные команды:*\n"
        f"/stats - статистика по водителям\n"
        f"/top - топ водителей по заказам\n"
        f"/search <имя> - поиск водителя\n"
        f"/new - новые водители (последние 30 дней)\n"
        f"/status <working/not_working/fired> - водители по статусу\n"
        f"/driver <id> - информация о конкретном водителе\n"
        f"/recent - последние обновления\n"
        f"/help - подробная справка",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий на кнопки"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "export":
        # Создаем контекст для вызова функции экспорта
        await export_drivers(query.message, context)

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

async def export_drivers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Экспорт всех водителей в CSV файл (с телефонами)"""
    if update.effective_user.id not in settings.ADMIN_IDS:
        await update.message.reply_text("⛔ У вас нет прав для этой команды")
        return
    
    await update.message.reply_text("📊 Собираю данные для экспорта...")
    
    db = SessionLocal()
    try:
        drivers = db.query(models.Driver).order_by(
            models.Driver.orders_count.desc()
        ).all()
        
        if not drivers:
            await update.message.reply_text("Нет данных для экспорта")
            return
        
        # Создаем CSV в памяти
        output = io.StringIO()
        writer = csv.writer(output, delimiter=';')
        
        # Заголовки (добавлена колонка "Телефон")
        writer.writerow([
            'ID', 'Имя', 'Фамилия', 'Статус', 'Заказы', 
            'Баланс', 'Валюта', 'Текущий статус', 
            'Дата регистрации', 'Последняя транзакция', 
            'Последнее обновление', 'Дата добавления',
            'Телефон'  # ← НОВАЯ КОЛОНКА
        ])
        
        # Данные
        for driver in drivers:
            writer.writerow([
                driver.driver_id,
                driver.first_name,
                driver.last_name,
                driver.work_status,
                driver.orders_count,
                driver.balance,
                driver.currency,
                driver.current_status,
                driver.created_date[:10] if driver.created_date else '',
                driver.last_transaction_date[:10] if driver.last_transaction_date else '',
                driver.last_updated.strftime('%Y-%m-%d %H:%M') if driver.last_updated else '',
                driver.created_at.strftime('%Y-%m-%d %H:%M') if driver.created_at else '',
                driver.phone if driver.phone else ''  # ← ТЕЛЕФОН
            ])
        
        # Конвертируем в bytes для отправки
        output_bytes = io.BytesIO()
        output_bytes.write(output.getvalue().encode('utf-8-sig'))
        output_bytes.seek(0)
        
        # Отправляем файл
        await update.message.reply_document(
            document=output_bytes,
            filename=f'drivers_export_{datetime.now().strftime("%Y%m%d_%H%M")}.csv',
            caption=f'📊 Экспорт данных о водителях\n'
                    f'Всего записей: {len(drivers)}\n'
                    f'📞 С телефонами: {len([d for d in drivers if d.phone])}\n'
                    f'Дата: {datetime.now().strftime("%Y-%m-%d %H:%M")}'
        )
        
    except Exception as e:
        logger.error(f"Export error: {e}")
        await update.message.reply_text(f"❌ Ошибка при экспорте: {str(e)}")
    finally:
        db.close()

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
        
        # Добавляем информацию о последнем сборе
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
        # Получаем топ-10 работающих водителей
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
            response += f"{i}. {driver.first_name} {driver.last_name}\n"
            response += f"   📦 Заказов: {driver.orders_count}\n"
            if driver.work_status == 'working':
                response += f"   🟢 Статус: Работает\n"
            response += "\n"
        
        await update.message.reply_text(response[:4096], parse_mode='Markdown')
        
    finally:
        db.close()

async def search_driver(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /search <имя или ID>"""
    if not context.args:
        await update.message.reply_text(
            "❓ Укажите имя или ID водителя\n"
            "Пример: /search Иван"
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
            response += f"👤 {driver.first_name} {driver.last_name}\n"
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
            response += f"👤 {driver.first_name} {driver.last_name}\n"
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
            response += f"👤 {driver.first_name} {driver.last_name}\n"
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
            f"*Имя:* {driver.first_name} {driver.last_name}\n"
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

async def update_phones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /update_phones - принудительное обновление телефонов (админ)"""
    if update.effective_user.id not in settings.ADMIN_IDS:
        await update.message.reply_text("⛔ У вас нет прав для этой команды")
        return
    
    await update.message.reply_text("📞 Начинаю обновление номеров телефонов в фоновом режиме...\n⚠️ Это может занять несколько минут. Я сообщу о завершении.")
    
    # Запускаем в фоновой задаче, не блокируя бота
    async def run_update():
        try:
            # Запускаем синхронную функцию в отдельном потоке
            result = await asyncio.to_thread(
                DataCollector().update_all_driver_phones,
                batch_size=100
            )
            await update.message.reply_text(
                f"✅ Обновление завершено!\n\n"
                f"📞 Обновлено номеров: {result['updated']}\n"
                f"❌ Ошибок: {len(result['errors'])}\n\n"
                f"_Номера телефонов сохранены в базе данных_",
                parse_mode='Markdown'
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка при обновлении: {str(e)}")
    
    # Запускаем фоновую задачу
    asyncio.create_task(run_update())

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help"""
    help_text = """
📚 *Подробная справка по командам:*

/stats - Общая статистика
   • Количество водителей по статусам
   • Среднее количество заказов
   • Информация о последнем обновлении

/top - Топ-10 работающих водителей по заказам

/search <имя или ID> - Поиск водителей
   • Поиск по имени или ID
   • Показывает первые 10 результатов

/new - Новые водители
   • Показывает водителей, добавленных за последние 30 дней

/status <working/not_working/fired> - Водители по статусу
   • Показывает до 20 водителей с указанным статусом

/driver <id> - Информация о конкретном водителе
   • Полная информация: имя, статус, заказы, баланс

/recent - История обновлений
   • Показывает последние 5 запусков сборщика

/help - Эта справка

*Примеры использования:*
/search Иван
/status working
/driver 123456789
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка неизвестных команд"""
    await update.message.reply_text(
        "❌ Неизвестная команда.\n"
        "Используйте /help для списка доступных команд"
    )

