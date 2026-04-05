from sqlalchemy.orm import Session
from sqlalchemy import or_, func  # ← добавляем or_ и func
from sqlalchemy import or_, func, update as sql_update
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging
from database import models

logger = logging.getLogger(__name__)

# ========== ВОДИТЕЛИ ==========

def save_driver(db: Session, driver_data: Dict[str, Any]) -> models.Driver:
    """Сохранить или обновить водителя"""
    driver = db.query(models.Driver).filter(
        models.Driver.driver_id == driver_data['driver_id']
    ).first()
    
    if driver:
        # Обновляем существующего
        for key, value in driver_data.items():
            if hasattr(driver, key):
                setattr(driver, key, value)
        driver.last_updated = datetime.utcnow()
    else:
        # Создаем нового
        driver = models.Driver(**driver_data)
        db.add(driver)
    
    db.commit()
    db.refresh(driver)
    return driver

def get_driver(db: Session, driver_id: str) -> Optional[models.Driver]:
    """Получить водителя по ID"""
    return db.query(models.Driver).filter(
        models.Driver.driver_id == driver_id
    ).first()

def get_all_drivers(db: Session, limit: int = 100, offset: int = 0) -> List[models.Driver]:
    """Получить список всех водителей"""
    return db.query(models.Driver).order_by(
        models.Driver.created_at.desc()
    ).offset(offset).limit(limit).all()

def get_drivers_by_status(db: Session, status: str) -> List[models.Driver]:
    """Получить водителей по статусу"""
    return db.query(models.Driver).filter(
        models.Driver.work_status == status
    ).all()

def get_drivers_for_update(db: Session, max_count: int = 100) -> List[models.Driver]:
    """Получить водителей, которым нужно обновить заказы"""
    now = datetime.utcnow()
    
    cutoff_date = now - timedelta(days=30)
    stale_date = now - timedelta(days=3)
    
    drivers = db.query(models.Driver).filter(
        models.Driver.work_status != 'fired',
        or_(
            models.Driver.created_at >= cutoff_date,
            or_(
                models.Driver.last_updated.is_(None),
                models.Driver.last_updated <= stale_date
            )
        )
    ).limit(max_count).all()
    
    return drivers

def update_driver_orders(
    db: Session, 
    driver_id: str, 
    orders_count: int,
    last_transaction_date: Optional[str] = None
) -> bool:
    """Обновить количество заказов водителя"""
    driver = db.query(models.Driver).filter(
        models.Driver.driver_id == driver_id
    ).first()
    
    if driver:
        # Обновляем только если новое значение >= старого
        if orders_count >= driver.orders_count:
            driver.orders_count = orders_count
            driver.last_updated = datetime.utcnow()
            if last_transaction_date:
                driver.last_transaction_date = last_transaction_date
            db.commit()
            return True
        else:
            # Логируем ошибку
            logger.warning(f"Orders count decreased for {driver_id}: {driver.orders_count} -> {orders_count}")
            return False
    return False

def get_driver_statistics(db: Session) -> Dict[str, Any]:
    """Получить статистику по водителям"""
    total = db.query(models.Driver).count()
    working = db.query(models.Driver).filter(
        models.Driver.work_status == 'working'
    ).count()
    not_working = db.query(models.Driver).filter(
        models.Driver.work_status == 'not_working'
    ).count()
    fired = db.query(models.Driver).filter(
        models.Driver.work_status == 'fired'
    ).count()
    
    # Новые водители за последние 30 дней
    cutoff = datetime.utcnow() - timedelta(days=30)
    new_drivers = db.query(models.Driver).filter(
        models.Driver.created_at >= cutoff
    ).count()
    
    # Среднее количество заказов
    from sqlalchemy import func
    avg_orders = db.query(func.avg(models.Driver.orders_count)).scalar() or 0
    
    return {
        'total': total,
        'working': working,
        'not_working': not_working,
        'fired': fired,
        'new_last_30_days': new_drivers,
        'avg_orders': int(avg_orders)
    }

def search_drivers(db: Session, query: str) -> List[models.Driver]:
    """Поиск водителей по имени или ID"""
    search_pattern = f"%{query}%"
    return db.query(models.Driver).filter(
        db.or_(
            models.Driver.driver_id.ilike(search_pattern),
            models.Driver.first_name.ilike(search_pattern),
            models.Driver.last_name.ilike(search_pattern)
        )
    ).limit(20).all()

def init_update_queue(db: Session):
    """Инициализирует очередь обновления из существующих водителей"""
    from datetime import datetime
    
    # Проверяем, есть ли записи
    count = db.query(models.UpdateQueue).count()
    if count > 0:
        logger.info(f"Queue already has {count} entries")
        return count
    
    # Получаем всех активных водителей
    drivers = db.query(models.Driver).filter(
        models.Driver.work_status != 'fired'
    ).all()
    
    added = 0
    for driver in drivers:
        priority = 0
        if driver.created_date:
            try:
                created = datetime.strptime(driver.created_date[:10], '%Y-%m-%d')
                if (datetime.utcnow() - created).days < 30:
                    priority = 1
            except:
                pass
        
        queue_entry = models.UpdateQueue(
            driver_id=driver.driver_id,
            priority=priority,
            last_updated=driver.last_updated or datetime.utcnow()
        )
        db.add(queue_entry)
        added += 1
    
    db.commit()
    logger.info(f"Initialized queue with {added} drivers")
    return added


def get_next_drivers_for_update(db: Session, batch_size: int = 100) -> List[models.UpdateQueue]:
    """
    Получает следующих водителей из очереди для обновления (FIFO)
    Сначала водители с высоким приоритетом, затем по дате последнего обновления
    """
    # Получаем водителей с высоким приоритетом (новые)
    priority_drivers = db.query(models.UpdateQueue).filter(
        models.UpdateQueue.priority == 1
    ).order_by(
        models.UpdateQueue.last_updated.asc()
    ).limit(batch_size).all()
    
    if len(priority_drivers) >= batch_size:
        return priority_drivers
    
    # Если высокоприоритетных меньше, добираем обычными
    remaining = batch_size - len(priority_drivers)
    regular_drivers = db.query(models.UpdateQueue).filter(
        models.UpdateQueue.priority == 0
    ).order_by(
        models.UpdateQueue.last_updated.asc()
    ).limit(remaining).all()
    
    return priority_drivers + regular_drivers

def update_queue_timestamp(db: Session, driver_id: str):
    """Обновляет время последнего обновления в очереди"""
    db.query(models.UpdateQueue).filter(
        models.UpdateQueue.driver_id == driver_id
    ).update({'last_updated': datetime.utcnow()})
    db.commit()

def add_driver_to_queue(db: Session, driver_id: str, is_new: bool = True):
    """Добавляет водителя в очередь"""
    existing = db.query(models.UpdateQueue).filter(
        models.UpdateQueue.driver_id == driver_id
    ).first()
    
    if existing:
        # Обновляем приоритет и время
        existing.priority = 1 if is_new else existing.priority
        existing.last_updated = datetime.utcnow()
    else:
        queue_entry = models.UpdateQueue(
            driver_id=driver_id,
            priority=1 if is_new else 0,
            last_updated=datetime.utcnow()
        )
        db.add(queue_entry)
    
    db.commit()

def get_queue_stats(db: Session) -> Dict[str, Any]:
    """Получает статистику очереди"""
    total = db.query(models.UpdateQueue).count()
    high_priority = db.query(models.UpdateQueue).filter(
        models.UpdateQueue.priority == 1
    ).count()
    low_priority = total - high_priority
    
    # Среднее время ожидания
    from sqlalchemy import func
    avg_wait = db.query(func.avg(
        func.extract('epoch', datetime.utcnow() - models.UpdateQueue.last_updated)
    )).scalar() or 0
    
    return {
        'total': total,
        'high_priority': high_priority,
        'low_priority': low_priority,
        'avg_wait_hours': round(avg_wait / 3600, 1)
    }

def get_drivers_needing_phone_update(db: Session, months: int = 1, limit: int = None) -> List[models.Driver]:
    """
    Получает водителей, которым нужно обновить номер телефона:
    1. У кого phone IS NULL (никогда не получали)
    2. У кого phone_updated_at старше months месяцев
    """
    cutoff_date = datetime.utcnow() - timedelta(days=months * 30)
    
    query = db.query(models.Driver).filter(
        db.or_(
            models.Driver.phone.is_(None),
            models.Driver.phone == '',
            models.Driver.phone_updated_at < cutoff_date
        )
    )
    
    if limit:
        query = query.limit(limit)
    
    return query.all()

def update_driver_phone(db: Session, driver_id: str, phone: str):
    """Обновляет номер телефона водителя"""
    driver = db.query(models.Driver).filter(
        models.Driver.driver_id == driver_id
    ).first()
    
    if driver:
        driver.phone = phone
        driver.phone_updated_at = datetime.utcnow()
        db.commit()
        return True
    return False

def get_api_calls_today(db: Session) -> int:
    """Возвращает количество API вызовов, сделанных сегодня"""
    today = datetime.utcnow().date()
    start_of_day = datetime(today.year, today.month, today.day)
    
    total_calls = db.query(models.CollectionLog).filter(
        models.CollectionLog.started_at >= start_of_day
    ).with_entities(
        db.func.sum(models.CollectionLog.api_calls_used)
    ).scalar() or 0
    
    return total_calls

# ========== ЛОГИ ==========

def create_collection_log(
    db: Session,
    status: str,
    new_drivers_added: int = 0,
    status_updated: int = 0,
    orders_updated: int = 0,
    api_calls_used: int = 0,
    errors: List[str] = None,
    error_message: str = None
) -> models.CollectionLog:
    """Создать запись о сборе данных"""
    log = models.CollectionLog(
        status=status,
        new_drivers_added=new_drivers_added,
        status_updated=status_updated,
        orders_updated=orders_updated,
        api_calls_used=api_calls_used,
        errors=errors or [],
        error_message=error_message,
        finished_at=datetime.utcnow()
    )
    db.add(log)
    db.commit()
    db.refresh(log)
    return log

def get_last_collection_log(db: Session) -> Optional[models.CollectionLog]:
    """Получить последний лог сбора"""
    return db.query(models.CollectionLog).order_by(
        models.CollectionLog.started_at.desc()
    ).first()

def get_collection_history(db: Session, limit: int = 10) -> List[models.CollectionLog]:
    """Получить историю сборов"""
    return db.query(models.CollectionLog).order_by(
        models.CollectionLog.started_at.desc()
    ).limit(limit).all()