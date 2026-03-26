{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "5086fcf5",
   "metadata": {},
   "outputs": [],
   "source": [
    "import os\n",
    "from dotenv import load_dotenv\n",
    "\n",
    "load_dotenv()\n",
    "\n",
    "class Settings:\n",
    "    \"\"\"Настройки приложения\"\"\"\n",
    "    \n",
    "    # Telegram Bot\n",
    "    TELEGRAM_BOT_TOKEN = os.getenv(\"TELEGRAM_BOT_TOKEN\")\n",
    "    \n",
    "    # Database\n",
    "    DATABASE_URL = os.getenv(\"DATABASE_URL\")\n",
    "    \n",
    "    # Yandex Taxi API\n",
    "    YA_API_KEY = os.getenv(\"YA_API_KEY\")\n",
    "    YA_CLIENT_ID = os.getenv(\"YA_CLIENT_ID\")\n",
    "    YA_PARK_ID = os.getenv(\"YA_PARK_ID\")\n",
    "    \n",
    "    # Параметры обновления (ваши оригинальные настройки)\n",
    "    DAYS_NEW_DRIVER_THRESHOLD = int(os.getenv(\"DAYS_NEW_DRIVER_THRESHOLD\", \"30\"))\n",
    "    DAYS_TRANSACTIONS_BACK = int(os.getenv(\"DAYS_TRANSACTIONS_BACK\", \"30\"))\n",
    "    DAYS_STATUS_UPDATE_WORKING = int(os.getenv(\"DAYS_STATUS_UPDATE_WORKING\", \"5\"))\n",
    "    DAYS_STATUS_UPDATE_NOT_WORKING = int(os.getenv(\"DAYS_STATUS_UPDATE_NOT_WORKING\", \"10\"))\n",
    "    MAX_API_CALLS_PER_DAY = int(os.getenv(\"MAX_API_CALLS_PER_DAY\", \"2500\"))\n",
    "    \n",
    "    # Collection settings\n",
    "    COLLECTION_INTERVAL = int(os.getenv(\"COLLECTION_INTERVAL\", \"3600\"))  # 1 hour\n",
    "    \n",
    "    # Admin IDs\n",
    "    ADMIN_IDS = [int(id) for id in os.getenv(\"ADMIN_IDS\", \"\").split(\",\") if id]\n",
    "\n",
    "settings = Settings()"
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
