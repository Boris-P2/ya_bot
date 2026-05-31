import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import time
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

from collector.main import DataCollector
from shared.config import settings
from database.session import SessionLocal, engine
from database import crud
from database.models import Base

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


def process_pending_invites_job():
    """Задача для обработки ожидающих приглашений"""
    logger.info("Processing pending invites...")
    db = SessionLocal()
    try:
        crud.process_pending_invites(db)
    except Exception as e:
        logger.error(f"Failed to process pending invites: {e}")
    finally:
        db.close()


def queue_stats_job():
    """Задача для логирования статистики очереди (опционально)"""
    db = SessionLocal()
    try:
        stats = crud.get_queue_stats(db)
        logger.info(f"Queue stats: total={stats['total']}, high_priority={stats['high_priority']}, avg_wait={stats['avg_wait_hours']}h")
    except Exception as e:
        logger.error(f"Failed to get queue stats: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    print("🚀 Starting Yandex Taxi Data Collector with Phone Updater...")
    
    # Проверяем переменные окружения
    if not settings.DATABASE_URL:
        logger.error("DATABASE_URL is not set!")
        sys.exit(1)
    
    if not settings.YA_API_KEY:
        logger.error("YA_API_KEY is not set!")
        sys.exit(1)
    
    # Инициализируем базу данных при старте
    init_database()
    
    # Планировщик
    scheduler = BackgroundScheduler()
    
    # Основной сбор (заказы) - каждые 6 часов
    scheduler.add_job(
        collect_job,
        'interval',
        hours=6,
        id='data_collection'
    )
    
    # Обновление телефонов - каждые 6 часов (со сдвигом 3 часа)
    scheduler.add_job(
        phone_update_job,
        'interval',
        hours=6,
        id='phone_update',
        next_run_time=datetime.now() + timedelta(hours=3)
    )
    
    # Обработка ожидающих приглашений - каждые 6 часов (со сдвигом 1 час)
    scheduler.add_job(
        process_pending_invites_job,
        'interval',
        hours=6,
        id='pending_invites',
        next_run_time=datetime.now() + timedelta(hours=1)
    )
    
    # Опционально: логирование статистики очереди раз в час
    scheduler.add_job(
        queue_stats_job,
        'interval',
        hours=1,
        id='queue_stats'
    )
    
    scheduler.start()
    logger.info("Scheduler started.")
    logger.info("  - Data collection: every 6 hours")
    logger.info("  - Phone update: every 6 hours (starts in 3h)")
    logger.info("  - Pending invites: every 6 hours (starts in 1h)")
    logger.info("  - Queue stats: every 1 hour")
    
    # Выполняем первый сбор сразу
    collect_job()
    
    # Держим процесс активным
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        scheduler.shutdown()
        logger.info("Scheduler stopped")