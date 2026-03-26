{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "facaa278",
   "metadata": {},
   "outputs": [],
   "source": [
    "import logging\n",
    "from telegram.ext import (\n",
    "    Application,\n",
    "    CommandHandler,\n",
    "    MessageHandler,\n",
    "    filters,\n",
    "    CallbackQueryHandler\n",
    ")\n",
    "from shared.config import settings\n",
    "from bot.handlers import (\n",
    "    start,\n",
    "    help_command,\n",
    "    get_recent,\n",
    "    search_data,\n",
    "    get_stats,\n",
    "    unknown\n",
    ")\n",
    "\n",
    "logging.basicConfig(\n",
    "    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',\n",
    "    level=logging.INFO\n",
    ")\n",
    "logger = logging.getLogger(__name__)\n",
    "\n",
    "def run_bot():\n",
    "    \"\"\"Запуск Telegram бота\"\"\"\n",
    "    logger.info(\"Starting Telegram bot...\")\n",
    "    \n",
    "    # Создаем приложение\n",
    "    application = Application.builder().token(settings.TELEGRAM_BOT_TOKEN).build()\n",
    "    \n",
    "    # Регистрируем команды\n",
    "    application.add_handler(CommandHandler(\"start\", start))\n",
    "    application.add_handler(CommandHandler(\"help\", help_command))\n",
    "    application.add_handler(CommandHandler(\"recent\", get_recent))\n",
    "    application.add_handler(CommandHandler(\"search\", search_data))\n",
    "    application.add_handler(CommandHandler(\"stats\", get_stats))\n",
    "    \n",
    "    # Обработчик неизвестных команд\n",
    "    application.add_handler(MessageHandler(filters.COMMAND, unknown))\n",
    "    \n",
    "    # Запускаем бота\n",
    "    logger.info(\"Bot is running...\")\n",
    "    application.run_polling(allowed_updates=Update.ALL_TYPES)\n",
    "\n",
    "if __name__ == \"__main__\":\n",
    "    run_bot()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.4"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
