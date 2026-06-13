#!/usr/bin/env python
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.session import engine
from sqlalchemy import text, inspect
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def add_columns():
    with engine.connect() as conn:
        # Проверяем существующие колонки
        inspector = inspect(engine)
        existing_columns = [col['name'] for col in inspector.get_columns('user_access')]
        
        # Добавляем admin_password
        if 'admin_password' not in existing_columns:
            conn.execute(text('ALTER TABLE user_access ADD COLUMN admin_password VARCHAR(100)'))
            logger.info('✅ Колонка admin_password добавлена')
        else:
            logger.info('⏭️ Колонка admin_password уже существует')
        
        # Добавляем consent_given
        if 'consent_given' not in existing_columns:
            conn.execute(text('ALTER TABLE user_access ADD COLUMN consent_given INTEGER DEFAULT 0'))
            logger.info('✅ Колонка consent_given добавлена')
        else:
            logger.info('⏭️ Колонка consent_given уже существует')
        
        # Добавляем consent_date
        if 'consent_date' not in existing_columns:
            conn.execute(text('ALTER TABLE user_access ADD COLUMN consent_date TIMESTAMP'))
            logger.info('✅ Колонка consent_date добавлена')
        else:
            logger.info('⏭️ Колонка consent_date уже существует')
        
        conn.commit()
        logger.info('✅ Все колонки добавлены')

if __name__ == '__main__':
    add_columns()