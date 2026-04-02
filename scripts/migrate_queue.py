
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from database.session import engine, SessionLocal
from sqlalchemy import text, inspect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_update_queue():
    """Безопасная миграция таблицы update_queue"""
    db = SessionLocal()
    
    try:
        # Проверяем, существует ли таблица
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        if 'update_queue' not in tables:
            logger.info("Таблица update_queue не существует, создаем новую")
            from database.models import Base
            from database import models
            Base.metadata.create_all(bind=engine, tables=[models.UpdateQueue.__table__])
            logger.info("Таблица update_queue создана")
            return
        
        # Проверяем, есть ли в таблице колонка 'status' (старая версия)
        columns = [col['name'] for col in inspector.get_columns('update_queue')]
        
        if 'status' in columns:
            logger.info("Обнаружена старая версия таблицы (с колонкой status)")
            
            # 1. Создаем временную таблицу с новыми полями
            logger.info("Создаю временную таблицу...")
            db.execute(text("""
                CREATE TABLE update_queue_new (
                    id SERIAL PRIMARY KEY,
                    driver_id VARCHAR(100) UNIQUE NOT NULL,
                    priority INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            db.commit()
            
            # 2. Переносим данные из старой таблицы
            logger.info("Переношу данные из старой таблицы...")
            
            # Проверяем, есть ли колонка priority в старой таблице
            has_priority = 'priority' in columns
            
            if has_priority:
                db.execute(text("""
                    INSERT INTO update_queue_new (driver_id, priority, last_updated, updated_at)
                    SELECT driver_id, COALESCE(priority, 0), last_updated, updated_at
                    FROM update_queue
                    WHERE driver_id IS NOT NULL
                """))
            else:
                # Если нет priority, устанавливаем значение по умолчанию
                db.execute(text("""
                    INSERT INTO update_queue_new (driver_id, priority, last_updated, updated_at)
                    SELECT driver_id, 0, last_updated, updated_at
                    FROM update_queue
                    WHERE driver_id IS NOT NULL
                """))
            db.commit()
            
            count = db.execute(text("SELECT COUNT(*) FROM update_queue_new")).scalar()
            logger.info(f"Перенесено {count} записей")
            
            # 3. Удаляем старую таблицу
            logger.info("Удаляю старую таблицу...")
            db.execute(text("DROP TABLE update_queue CASCADE"))
            db.commit()
            
            # 4. Переименовываем новую таблицу
            logger.info("Переименовываю новую таблицу...")
            db.execute(text("ALTER TABLE update_queue_new RENAME TO update_queue"))
            db.commit()
            
            logger.info("Миграция завершена успешно!")
            
        else:
            # Проверяем структуру новой таблицы
            if 'priority' not in columns:
                logger.warning("Таблица update_queue имеет нестандартную структуру")
                logger.info("Добавляю недостающие колонки...")
                
                if 'priority' not in columns:
                    db.execute(text("ALTER TABLE update_queue ADD COLUMN priority INTEGER DEFAULT 0"))
                if 'updated_at' not in columns:
                    db.execute(text("ALTER TABLE update_queue ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"))
                db.commit()
                logger.info("Колонки добавлены")
            else:
                logger.info("Таблица update_queue уже имеет правильную структуру")
        
    except Exception as e:
        logger.error(f"Ошибка миграции: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def init_queue_from_drivers():
    """Заполняет очередь из существующих водителей"""
    db = SessionLocal()
    
    try:
        # Проверяем, есть ли записи в очереди
        count = db.execute(text("SELECT COUNT(*) FROM update_queue")).scalar()
        
        if count > 0:
            logger.info(f"В очереди уже есть {count} записей, инициализация не требуется")
            return
        
        # Получаем всех активных водителей
        logger.info("Заполняю очередь из существующих водителей...")
        from database import models
        
        drivers = db.query(models.Driver).filter(
            models.Driver.work_status != 'fired'
        ).all()
        
        added = 0
        for driver in drivers:
            # Определяем приоритет (новые водители получают высокий)
            priority = 0
            if driver.created_date:
                try:
                    created = datetime.strptime(driver.created_date[:10], '%Y-%m-%d')
                    if (datetime.utcnow() - created).days < 30:
                        priority = 1
                except:
                    pass
            
            # Добавляем в очередь
            db.execute(text("""
                INSERT INTO update_queue (driver_id, priority, last_updated, updated_at)
                VALUES (:driver_id, :priority, :last_updated, :updated_at)
                ON CONFLICT (driver_id) DO NOTHING
            """), {
                'driver_id': driver.driver_id,
                'priority': priority,
                'last_updated': driver.last_updated or datetime.utcnow(),
                'updated_at': datetime.utcnow()
            })
            added += 1
        
        db.commit()
        logger.info(f"Добавлено {added} водителей в очередь")
        
    except Exception as e:
        logger.error(f"Ошибка инициализации очереди: {e}")
        db.rollback()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    from datetime import datetime
    
    print("=" * 50)
    print("Миграция таблицы update_queue")
    print("=" * 50)
    
    # Запускаем миграцию
    migrate_update_queue()
    
    # Заполняем очередь из водителей
    init_queue_from_drivers()
    
    print("\n✅ Миграция завершена!")