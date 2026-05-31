from sqlalchemy.orm import Session
from sqlalchemy import or_, func, and_, text
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging
import re
from database import models

logger = logging.getLogger(__name__)


# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

def normalize_phone(phone: str) -> str:
    """Нормализует номер телефона: удаляет пробелы, скобки, дефисы"""
    if not phone:
        return ''
    return re.sub(r'[^\d+]', '', phone)


# ========== ВОДИТЕЛИ ==========

def get_drivers_for_update(db: Session, max_count: int = 100) -> List[models.Driver]:
    """Получить водителей, которым нужно обновить заказы (старая логика)"""
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


def save_driver(db: Session, driver_data: Dict[str, Any]) -> models.Driver:
    """Сохранить или обновить водителя"""
    driver = db.query(models.Driver).filter(
        models.Driver.driver_id == driver_data['driver_id']
    ).first()
    
    if driver:
        for key, value in driver_data.items():
            if hasattr(driver, key):
                setattr(driver, key, value)
        driver.last_updated = datetime.utcnow()
    else:
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


def get_driver_by_telegram_id(db: Session, telegram_id: int) -> Optional[models.Driver]:
    """Получить водителя по Telegram ID"""
    return db.query(models.Driver).filter(
        models.Driver.telegram_id == telegram_id
    ).first()


def get_driver_by_phone(db: Session, phone: str) -> Optional[models.Driver]:
    """Получить водителя по номеру телефона (с нормализацией)"""
    normalized = normalize_phone(phone)
    if not normalized:
        return None
    return db.query(models.Driver).filter(
        models.Driver.phone == normalized
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
        driver.orders_count = orders_count
        driver.last_updated = datetime.utcnow()
        if last_transaction_date:
            driver.last_transaction_date = last_transaction_date
        db.commit()
        return True
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
    
    cutoff = datetime.utcnow() - timedelta(days=30)
    new_drivers = db.query(models.Driver).filter(
        models.Driver.created_at >= cutoff
    ).count()
    
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
    """Поиск водителей по фамилии или ID"""
    search_pattern = f"%{query}%"
    return db.query(models.Driver).filter(
        or_(
            models.Driver.driver_id.ilike(search_pattern),
            models.Driver.last_name.ilike(search_pattern)
        )
    ).limit(20).all()


# ========== ОЧЕРЕДЬ ОБНОВЛЕНИЯ (FIFO) ==========

def get_next_drivers_for_update(db: Session, batch_size: int = 100) -> List[models.UpdateQueue]:
    """Получает следующих водителей из очереди (FIFO)"""
    priority_drivers = db.query(models.UpdateQueue).filter(
        models.UpdateQueue.priority == 1
    ).order_by(
        models.UpdateQueue.last_updated.asc()
    ).limit(batch_size).all()
    
    if len(priority_drivers) >= batch_size:
        return priority_drivers
    
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


def init_update_queue(db: Session) -> int:
    """Инициализирует очередь обновления из существующих водителей"""
    count = db.query(models.UpdateQueue).count()
    if count > 0:
        logger.info(f"Queue already has {count} entries")
        return count
    
    drivers = db.query(models.Driver).filter(
        models.Driver.work_status != 'fired'
    ).all()
    
    added = 0
    for driver in drivers:
        # Проверяем наличие реального телефона (не пустой строки)
        has_phone = driver.phone and driver.phone != ''
        priority = 1 if has_phone else 0
        
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


def get_queue_stats(db: Session) -> Dict[str, Any]:
    """Получает статистику очереди"""
    total = db.query(models.UpdateQueue).count()
    high_priority = db.query(models.UpdateQueue).filter(
        models.UpdateQueue.priority == 1
    ).count()
    low_priority = total - high_priority
    
    avg_wait = db.query(func.avg(
        func.extract('epoch', datetime.utcnow() - models.UpdateQueue.last_updated)
    )).scalar() or 0
    
    return {
        'total': total,
        'high_priority': high_priority,
        'low_priority': low_priority,
        'avg_wait_hours': round(avg_wait / 3600, 1)
    }


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


# ========== ПОЛЬЗОВАТЕЛИ (TELEGRAM) ==========

def add_or_update_user(
    db: Session, 
    telegram_id: int, 
    username: str = None, 
    is_admin: bool = False
) -> models.UserAccess:
    """Добавляет или обновляет пользователя Telegram"""
    user = db.query(models.UserAccess).filter(
        models.UserAccess.telegram_id == telegram_id
    ).first()
    
    if user:
        user.username = username or user.username
        user.is_admin = 1 if is_admin else user.is_admin
        user.last_active = datetime.utcnow()
    else:
        user = models.UserAccess(
            telegram_id=telegram_id,
            username=username,
            is_admin=1 if is_admin else 0
        )
        db.add(user)
    
    db.commit()
    db.refresh(user)
    return user


def get_user(db: Session, telegram_id: int) -> Optional[models.UserAccess]:
    """Получить пользователя по Telegram ID"""
    return db.query(models.UserAccess).filter(
        models.UserAccess.telegram_id == telegram_id
    ).first()


# ========== РЕФЕРАЛЬНЫЕ СВЯЗИ ==========

def get_referral_count(db: Session, driver_id: str) -> int:
    """Получает количество приглашённых водителей"""
    return db.query(models.Referral).filter(
        models.Referral.referrer_id == driver_id
    ).count()


def create_referral(db: Session, referrer_id: str, referred_id: str) -> Optional[models.Referral]:
    """Создаёт новую реферальную связь"""
    existing = db.query(models.Referral).filter(
        and_(
            models.Referral.referrer_id == referrer_id,
            models.Referral.referred_id == referred_id
        )
    ).first()
    
    if existing:
        logger.warning(f"Referral already exists: {referrer_id} -> {referred_id}")
        return None
    
    if referrer_id == referred_id:
        logger.warning("Cannot refer yourself")
        return None
    
    referral = models.Referral(
        referrer_id=referrer_id,
        referred_id=referred_id,
        status='pending'
    )
    db.add(referral)
    db.commit()
    db.refresh(referral)
    return referral


def get_referrals_by_driver(db: Session, driver_id: str, status: str = None) -> List[models.Referral]:
    """Получает все приглашения водителя (кого он пригласил)"""
    query = db.query(models.Referral).filter(models.Referral.referrer_id == driver_id)
    if status:
        query = query.filter(models.Referral.status == status)
    return query.order_by(models.Referral.invited_at.desc()).all()


def get_referrer_by_driver(db: Session, driver_id: str) -> Optional[models.Referral]:
    """Кто пригласил этого водителя"""
    return db.query(models.Referral).filter(
        models.Referral.referred_id == driver_id,
        models.Referral.status != 'rejected'
    ).first()


def check_and_complete_referrals(db: Session, driver_id: str, orders_count: int, threshold: int = 100):
    """Проверяет, не выполнил ли водитель условие для награды и автоматически создаёт награду"""
    referrals = get_referrals_by_driver(db, driver_id, status='pending')
    
    completed = []
    for referral in referrals:
        referred = db.query(models.Driver).filter(
            models.Driver.driver_id == referral.referred_id
        ).first()
        
        if referred and referred.orders_count >= threshold:
            referral.status = 'completed'
            referral.completed_at = datetime.utcnow()
            db.commit()
            completed.append(referral)
            
            # Автоматически создаём награду для пригласившего
            complete_referral_and_reward(db, referral.id, driver_id)
    
    return completed


def create_reward(db: Session, referral_id: int, driver_id: str, amount: int = 100) -> models.ReferralReward:
    """Создаёт запись о награде"""
    reward = models.ReferralReward(
        referral_id=referral_id,
        driver_id=driver_id,
        amount=amount,
        status='pending'
    )
    db.add(reward)
    db.commit()
    db.refresh(reward)
    return reward


def get_reward_stats(db: Session, driver_id: str) -> Dict[str, Any]:
    """Статистика наград водителя"""
    total_reward = db.query(func.sum(models.ReferralReward.amount)).filter(
        models.ReferralReward.driver_id == driver_id,
        models.ReferralReward.status == 'paid'
    ).scalar() or 0
    
    pending_reward = db.query(func.sum(models.ReferralReward.amount)).filter(
        models.ReferralReward.driver_id == driver_id,
        models.ReferralReward.status == 'pending'
    ).scalar() or 0
    
    return {
        'total': int(total_reward),
        'pending': int(pending_reward)
    }


def complete_referral_and_reward(db: Session, referral_id: int, driver_id: str, amount: int = 100) -> Optional[models.ReferralReward]:
    """Завершает реферальную связь и создаёт награду"""
    referral = db.query(models.Referral).filter(
        models.Referral.id == referral_id
    ).first()
    
    if referral and referral.status == 'completed':
        referral.status = 'rewarded'
        referral.rewarded_at = datetime.utcnow()
        
        reward = models.ReferralReward(
            referral_id=referral_id,
            driver_id=driver_id,
            amount=amount,
            status='paid'
        )
        db.add(reward)
        db.commit()
        return reward
    
    return None


# ========== ОЖИДАЮЩИЕ ПРИГЛАШЕНИЯ (PENDING INVITES) ==========

def get_pending_invite_by_phone(db: Session, referrer_id: str, phone: str) -> Optional[models.PendingInvite]:
    """Получает ожидающее приглашение по номеру телефона"""
    normalized_phone = normalize_phone(phone)
    return db.query(models.PendingInvite).filter(
        models.PendingInvite.referrer_id == referrer_id,
        models.PendingInvite.phone == normalized_phone,
        models.PendingInvite.status == 'pending'
    ).first()


def count_pending_invites(db: Session, referrer_id: str) -> int:
    """Считает количество активных ожидающих приглашений"""
    return db.query(models.PendingInvite).filter(
        models.PendingInvite.referrer_id == referrer_id,
        models.PendingInvite.status == 'pending'
    ).count()


def create_pending_invite(db: Session, referrer_id: str, phone: str) -> models.PendingInvite:
    """Создаёт ожидающее приглашение"""
    normalized_phone = normalize_phone(phone)
    pending_invite = models.PendingInvite(
        referrer_id=referrer_id,
        phone=normalized_phone,
        status='pending',
        invited_at=datetime.utcnow()
    )
    db.add(pending_invite)
    db.commit()
    db.refresh(pending_invite)
    return pending_invite


def get_pending_invites_by_referrer(db: Session, referrer_id: str) -> List[models.PendingInvite]:
    """Получает все ожидающие приглашения по referrer_id"""
    return db.query(models.PendingInvite).filter(
        models.PendingInvite.referrer_id == referrer_id,
        models.PendingInvite.status == 'pending'
    ).order_by(models.PendingInvite.invited_at.asc()).all()


def get_all_pending_invites(db: Session) -> List[models.PendingInvite]:
    """Получает все активные ожидающие приглашения"""
    return db.query(models.PendingInvite).filter(
        models.PendingInvite.status == 'pending'
    ).all()


def cancel_pending_invite(db: Session, invite_id: int):
    """Отменяет ожидающее приглашение"""
    db.query(models.PendingInvite).filter(
        models.PendingInvite.id == invite_id
    ).update({
        'status': 'cancelled',
        'cancelled_at': datetime.utcnow()
    })
    db.commit()


def complete_pending_invite(db: Session, invite_id: int):
    """Завершает ожидающее приглашение (водитель найден)"""
    db.query(models.PendingInvite).filter(
        models.PendingInvite.id == invite_id
    ).update({'status': 'completed'})
    db.commit()


def process_pending_invites(db: Session):
    """Обрабатывает все ожидающие приглашения: проверяет, появился ли водитель в базе"""
    pending_invites = get_all_pending_invites(db)
    
    for invite in pending_invites:
        # Ищем водителя по номеру телефона
        driver = get_driver_by_phone(db, invite.phone)
        
        if driver:
            # Создаём реферальную связь
            referral = create_referral(db, invite.referrer_id, driver.driver_id)
            if referral:
                complete_pending_invite(db, invite.id)
                logger.info(f"Pending invite completed: {invite.referrer_id} -> {driver.driver_id}")
    
    # Отменяем приглашения старше 7 дней
    cutoff_date = datetime.utcnow() - timedelta(days=7)
    old_invites = db.query(models.PendingInvite).filter(
        models.PendingInvite.status == 'pending',
        models.PendingInvite.invited_at < cutoff_date
    ).all()
    
    for invite in old_invites:
        cancel_pending_invite(db, invite.id)
        logger.info(f"Pending invite cancelled (expired): {invite.referrer_id} -> {invite.phone}")