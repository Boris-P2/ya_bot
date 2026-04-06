import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import time
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from collector.main import DataCollector
from shared.config import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def init_database():
    """Создание таблиц при запуске"""
    try:
        logger.info("Checking database tables...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables ready")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise

def collect_job():
    """Задача для периодического сбора данных (заказы)"""
    logger.info("Starting scheduled data collection...")
    collector = DataCollector()
    result = collector.run_full_update()
    logger.info(f"Collection completed: {result}")

def phone_update_job():
    """Задача для периодического обновления телефонов"""
    logger.info("Starting scheduled phone update...")
    collector = DataCollector()
    result = collector.update_all_driver_phones(batch_size=500, days_stale=30)
    logger.info(f"Phone update completed: {result['updated']} updated")

if __name__ == "__main__":
    print("🚀 Starting Yandex Taxi Data Collector with Phone Updater...")
    
    # Проверяем переменные окружения
    if not settings.DATABASE_URL:
        logger.error("DATABASE_URL is not set!")
        sys.exit(1)
    
    if not settings.YA_API_KEY:
        logger.error("YA_API_KEY is not set!")
        sys.exit(1)
    
    # Планировщик для основного сбора (заказы) - каждые 6 часов
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        collect_job,
        'interval',
        hours=6,
        id='data_collection'
    )
    
    # Планировщик для обновления телефонов - каждые 6 часов (со сдвигом 3 часа)
    scheduler.add_job(
        phone_update_job,
        'interval',
        hours=6,
        id='phone_update',
        next_run_time=datetime.now() + timedelta(hours=3)
    )
    
    scheduler.start()
    logger.info("Scheduler started. Collection interval: 6 hours, Phone update interval: 6 hours")
    
    # Выполняем первый сбор сразу
    collect_job()
    
    # Держим процесс активным
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        scheduler.shutdown()
        logger.info("Scheduler stopped")