#!/usr/bin/env python
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.session import SessionLocal
from sqlalchemy import text

def clear_first_names():
    db = SessionLocal()
    try:
        result = db.execute(text("UPDATE drivers SET first_name = ''"))
        db.commit()
        print(f"✅ Очищено имён у {result.rowcount} водителей")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    clear_first_names()