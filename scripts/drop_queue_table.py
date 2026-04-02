import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.session import engine
from sqlalchemy import text

def drop_update_queue():
    """Удаляет таблицу update_queue"""
    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS update_queue CASCADE"))
            conn.commit()
            print("✅ Таблица update_queue успешно удалена")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    drop_update_queue()