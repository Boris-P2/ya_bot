from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator
from shared.config import settings
import logging

logger = logging.getLogger(__name__)

# Адаптация URL для psycopg3
DATABASE_URL = settings.DATABASE_URL

# Если используется psycopg3, меняем схему подключения
if DATABASE_URL and DATABASE_URL.startswith('postgresql://'):
    DATABASE_URL = DATABASE_URL.replace('postgresql://', 'postgresql+psycopg://')
    logger.info(f"Adapted database URL for psycopg3")

# Проверяем, что URL задан
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set!")

# Создаем engine для подключения к БД
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,      # Проверка соединения перед использованием
    pool_size=5,              # Размер пула соединений
    max_overflow=10,         # Максимум дополнительных соединений
    echo=False               # Отключаем логирование SQL (для продакшена)
)

# Создаем фабрику сессий
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db() -> Generator[Session, None, None]:
    """
    Dependency для получения сессии базы данных.
    Используется в обработчиках бота и в сборщике данных.
    """
    db = SessionLocal()
    try:
        # Проверяем соединение
        db.execute("SELECT 1")
        yield db
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        db.close()