import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    """Настройки приложения"""
    
    # Telegram Bot
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    
    # Database
    DATABASE_URL = os.getenv("DATABASE_URL")
    
    # Yandex Taxi API
    YA_API_KEY = os.getenv("YA_API_KEY")
    YA_CLIENT_ID = os.getenv("YA_CLIENT_ID")
    YA_PARK_ID = os.getenv("YA_PARK_ID")
    
    # Параметры обновления (ваши оригинальные настройки)
    DAYS_NEW_DRIVER_THRESHOLD = int(os.getenv("DAYS_NEW_DRIVER_THRESHOLD", "30"))
    DAYS_TRANSACTIONS_BACK = int(os.getenv("DAYS_TRANSACTIONS_BACK", "30"))
    DAYS_STATUS_UPDATE_WORKING = int(os.getenv("DAYS_STATUS_UPDATE_WORKING", "5"))
    DAYS_STATUS_UPDATE_NOT_WORKING = int(os.getenv("DAYS_STATUS_UPDATE_NOT_WORKING", "10"))
    MAX_API_CALLS_PER_DAY = int(os.getenv("MAX_API_CALLS_PER_DAY", "2500"))
    
    # Collection settings
    COLLECTION_INTERVAL = int(os.getenv("COLLECTION_INTERVAL", "3600"))  # 1 hour
    
    # Admin IDs
    ADMIN_IDS = [int(id) for id in os.getenv("ADMIN_IDS", "").split(",") if id]

settings = Settings()