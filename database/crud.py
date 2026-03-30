from sqlalchemy.orm import Session
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
    
    # Логика определения приоритета:
    # 1. Новые водители (менее 30 дней)
    # 2. Те, у кого last_updated > 3 дня
    # 3. Исключаем уволенных
    
    cutoff_date = now - timedelta(days=30)
    stale_date = now - timedelta(days=3)
    
    drivers = db.query(models.Driver).filter(
        models.Driver.work_status != 'fired',
        db.or_(
            models.Driver.created_at >= cutoff_date,  # Новые водители
            db.or_(
                models.Driver.last_updated.is_(None),
                models.Driver.last_updated <= stale_date  # Давно не обновлялись
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