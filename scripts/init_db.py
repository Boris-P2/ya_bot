import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import Base
from database.session import engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_database():
    """Создание всех таблиц в базе данных"""
    logger.info("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    logger.info("Database initialization completed!")

def drop_database():
    """Удаление всех таблиц (осторожно!)"""
    confirm = input("Are you sure you want to drop all tables? (yes/no): ")
    if confirm.lower() == 'yes':
        logger.warning("Dropping all tables...")
        Base.metadata.drop_all(bind=engine)
        logger.info("All tables dropped")
    else:
        logger.info("Operation cancelled")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'drop':
        drop_database()
    else:
        init_database()