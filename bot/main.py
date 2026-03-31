import logging
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    CallbackQueryHandler  # ← Убедитесь, что этот импорт есть
)
from shared.config import settings
from bot.handlers import (
    start,
    help_command,
    get_recent_updates,
    get_stats,
    get_top_drivers,
    search_driver,
    get_new_drivers,
    get_drivers_by_status,
    get_driver_info,
    unknown,
    export_drivers,
    button_callback
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def run_bot():
    """Запуск Telegram бота"""
    logger.info("Starting Telegram bot...")
    
    # Проверяем наличие токена
    if not settings.TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set!")
        return
    
    # Создаем приложение
    application = Application.builder().token(settings.TELEGRAM_BOT_TOKEN).build()
    
    # Регистрируем команды
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("stats", get_stats))
    application.add_handler(CommandHandler("top", get_top_drivers))
    application.add_handler(CommandHandler("search", search_driver))
    application.add_handler(CommandHandler("new", get_new_drivers))
    application.add_handler(CommandHandler("status", get_drivers_by_status))
    application.add_handler(CommandHandler("driver", get_driver_info))
    application.add_handler(CommandHandler("recent", get_recent_updates))
    application.add_handler(CommandHandler("export", export_drivers))

    # Обработчик кнопок
    application.add_handler(CallbackQueryHandler(button_callback))
    
    # Обработчик неизвестных команд
    application.add_handler(MessageHandler(filters.COMMAND, unknown))
    
    # Запускаем бота
    logger.info("Bot is running. Press Ctrl+C to stop...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    run_bot()
