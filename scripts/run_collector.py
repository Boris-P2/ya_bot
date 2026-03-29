import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import time
from apscheduler.schedulers.background import BackgroundScheduler
from collector.main import DataCollector
from shared.config import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def collect_job():
    """Задача для периодического сбора данных"""
    logger.info("Starting scheduled data collection...")
    collector = DataCollector()
    result = collector.run_full_update()
    logger.info(f"Collection completed: {result}")

if __name__ == "__main__":
    print("🚀 Starting Yandex Taxi Data Collector...")
    
    # Запускаем планировщик
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        collect_job,
        'interval',
        seconds=settings.COLLECTION_INTERVAL,
        id='data_collection'
    )
    scheduler.start()
    logger.info(f"Scheduler started. Collection interval: {settings.COLLECTION_INTERVAL} seconds")
    
    # Выполняем первый сбор сразу
    collect_job()
    
    # Держим процесс активным
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        scheduler.shutdown()
        logger.info("Scheduler stopped")