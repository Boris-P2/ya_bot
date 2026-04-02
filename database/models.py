from sqlalchemy import Column, Integer, String, DateTime, Float, JSON, Text, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class Driver(Base):
    """Модель водителя"""
    __tablename__ = 'drivers'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    driver_id = Column(String(100), unique=True, index=True, nullable=False)
    first_name = Column(String(100))
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
    
    # Метаданные
    priority_score = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

class UpdateQueue(Base):
    """Очередь обновления заказов (FIFO)"""
    __tablename__ = 'update_queue'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    driver_id = Column(String(100), unique=True, index=True, nullable=False)
    last_updated = Column(DateTime, default=datetime.utcnow)
    priority = Column(Integer, default=0)  # 0 - обычный, 1 - высокий приоритет (новые)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

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
    """Очередь для приоритетного обновления"""
    __tablename__ = 'update_queue'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    driver_id = Column(String(100), index=True)
    priority_score = Column(Integer, default=0)
    queued_at = Column(DateTime, default=datetime.utcnow)
    processed_at = Column(DateTime)
    status = Column(String(20), default='pending')  # pending, processing, completed, failed