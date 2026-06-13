from sqlalchemy import Column, Integer, String, DateTime, Float, JSON, Text, BigInteger, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class Driver(Base):
    """Модель водителя"""
    __tablename__ = 'drivers'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    driver_id = Column(String(100), unique=True, index=True, nullable=False)
    first_name = Column(String(100), default='')  # обезличено
    last_name = Column(String(100))
    created_date = Column(String(50))
    work_status = Column(String(50), index=True)
    balance = Column(String(50))
    currency = Column(String(10))
    current_status = Column(String(50))
    last_transaction_date = Column(String(50))
    orders_count = Column(Integer, default=0)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_status_updated = Column(DateTime, default=datetime.utcnow)
    phone = Column(String(20), nullable=True)
    phone_updated_at = Column(DateTime, nullable=True)
    telegram_id = Column(Integer, nullable=True, unique=True)  # для привязки Telegram

    # Метаданные
    priority_score = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)


class CollectionLog(Base):
    """Логирование сборов"""
    __tablename__ = 'collection_logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime)
    status = Column(String(20))  # success, failed
    new_drivers_added = Column(Integer, default=0)
    status_updated = Column(Integer, default=0)
    orders_updated = Column(Integer, default=0)
    api_calls_used = Column(Integer, default=0)
    errors = Column(JSON, default=list)
    error_message = Column(Text)


class UpdateQueue(Base):
    """Очередь для приоритетного обновления (FIFO)"""
    __tablename__ = 'update_queue'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    driver_id = Column(String(100), unique=True, index=True, nullable=False)
    priority = Column(Integer, default=0)  # 0 - обычный, 1 - высокий приоритет (новые)
    last_updated = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class UserAccess(Base):
    """Таблица для управления доступом пользователей Telegram"""
    __tablename__ = 'user_access'
    
    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(Integer, unique=True, index=True)
    username = Column(String(100))
    is_admin = Column(Integer, default=0)  # 0 - обычный, 1 - админ
    admin_password = Column(String(100), nullable=True)
    allowed_data_types = Column(JSON, default=list)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_active = Column(DateTime, default=datetime.utcnow)
    
    # Новые поля для согласия на обработку данных
    consent_given = Column(Integer, default=0)  # 0 - нет, 1 - да
    consent_date = Column(DateTime, nullable=True)  # дата и время согласия (UTC)


class Referral(Base):
    """Модель реферальных связей"""
    __tablename__ = 'referrals'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    referrer_id = Column(String(100), ForeignKey('drivers.driver_id'), nullable=False)
    referrer_phone = Column(String(20), nullable=True)  # ← добавить
    referred_phone = Column(String(20), nullable=False)  # ← добавить
    referred_id = Column(String(100), ForeignKey('drivers.driver_id'), nullable=True)  # ← сделать nullable
    status = Column(String(20), default='pending')
    referrer_confirmed = Column(Integer, default=0)
    referred_confirmed = Column(Integer, default=0)
    invited_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    rewarded_at = Column(DateTime, nullable=True)
    
    __table_args__ = (UniqueConstraint('referrer_id', 'referred_phone', name='unique_referral'),)


class ReferralReward(Base):
    """История наград"""
    __tablename__ = 'referral_rewards'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    referral_id = Column(Integer, ForeignKey('referrals.id'))
    driver_id = Column(String(100), ForeignKey('drivers.driver_id'))
    amount = Column(Integer, default=100)
    rewarded_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String(20), default='pending')  # pending, paid

class PendingInvite(Base):
    """Ожидающие приглашения (водитель ещё не в базе)"""
    __tablename__ = 'pending_invites'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    referrer_id = Column(String(100), nullable=False)
    phone = Column(String(20), nullable=False)
    invited_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String(20), default='pending')  # pending, cancelled, completed
    cancelled_at = Column(DateTime, nullable=True)
