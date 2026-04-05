#!/usr/bin/env python
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.session import engine
from sqlalchemy import text

def add_phone_column():
    """Добавляет колонку phone в таблицу drivers"""
    try:
        with engine.connect() as conn:
            # Проверяем, существует ли колонка
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'drivers' AND column_name = 'phone'
            """))
            
            if result.fetchone():
                print("✅ Колонка phone уже существует")
                return
            
            # Добавляем колонку phone
            conn.execute(text("""
                ALTER TABLE drivers 
                ADD COLUMN phone VARCHAR(20)
            """))
            conn.commit()
            print("✅ Колонка phone успешно добавлена")
            
            # Добавляем колонку phone_updated_at (опционально)
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'drivers' AND column_name = 'phone_updated_at'
            """))
            
            if not result.fetchone():
                conn.execute(text("""
                    ALTER TABLE drivers 
                    ADD COLUMN phone_updated_at TIMESTAMP
                """))
                conn.commit()
                print("✅ Колонка phone_updated_at успешно добавлена")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    add_phone_column()