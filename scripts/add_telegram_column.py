#!/usr/bin/env python
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.session import engine
from sqlalchemy import text

def add_telegram_column():
    with engine.connect() as conn:
        # Добавляем колонку telegram_id
        conn.execute(text("ALTER TABLE drivers ADD COLUMN IF NOT EXISTS telegram_id INTEGER UNIQUE"))
        conn.commit()
        print("✅ Колонка telegram_id добавлена")

if __name__ == "__main__":
    add_telegram_column()