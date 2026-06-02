
"""
Скрипт для очистки реферальных данных
Запуск: python scripts/clear_referral_data.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from database.session import engine, SessionLocal
from sqlalchemy import text

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def clear_referral_data():
    """Очищает все данные реферальной системы"""
    
    db = SessionLocal()
    
    try:
        logger.info("=" * 50)
        logger.info("Начинаем очистку реферальных данных")
        logger.info("=" * 50)
        
        # 1. Очищаем таблицу referral_rewards (награды)
        logger.info("1. Очищаем таблицу referral_rewards...")
        rewards_count = db.execute(text("SELECT COUNT(*) FROM referral_rewards")).scalar()
        db.execute(text("TRUNCATE TABLE referral_rewards RESTART IDENTITY CASCADE"))
        logger.info(f"   ✅ Удалено {rewards_count} записей из referral_rewards")
        
        # 2. Очищаем таблицу referrals (приглашения)
        logger.info("2. Очищаем таблицу referrals...")
        referrals_count = db.execute(text("SELECT COUNT(*) FROM referrals")).scalar()
        db.execute(text("TRUNCATE TABLE referrals RESTART IDENTITY CASCADE"))
        logger.info(f"   ✅ Удалено {referrals_count} записей из referrals")
        
        # 3. Очищаем таблицу pending_invites (ожидающие приглашения)
        logger.info("3. Очищаем таблицу pending_invites...")
        pending_count = db.execute(text("SELECT COUNT(*) FROM pending_invites")).scalar()
        db.execute(text("TRUNCATE TABLE pending_invites RESTART IDENTITY CASCADE"))
        logger.info(f"   ✅ Удалено {pending_count} записей из pending_invites")
        
        # 4. Очищаем поле telegram_id в таблице drivers (привязка Telegram)
        logger.info("4. Очищаем привязку Telegram в таблице drivers...")
        drivers_with_tg = db.execute(text("SELECT COUNT(*) FROM drivers WHERE telegram_id IS NOT NULL")).scalar()
        db.execute(text("UPDATE drivers SET telegram_id = NULL"))
        logger.info(f"   ✅ Очищено {drivers_with_tg} привязок Telegram")
        
        # 5. Очищаем поле admin_password в таблице user_access
        logger.info("5. Очищаем пароли администраторов в таблице user_access...")
        db.execute(text("UPDATE user_access SET admin_password = NULL"))
        logger.info(f"   ✅ Пароли администраторов очищены")
        
        # Фиксируем изменения
        db.commit()
        
        logger.info("=" * 50)
        logger.info("✅ Очистка реферальных данных завершена!")
        logger.info("=" * 50)
        
        # Выводим статистику
        logger.info("\n📊 Статистика после очистки:")
        logger.info(f"   • drivers: осталось {db.execute(text('SELECT COUNT(*) FROM drivers')).scalar()} записей")
        logger.info(f"   • referrals: 0 записей")
        logger.info(f"   • referral_rewards: 0 записей")
        logger.info(f"   • pending_invites: 0 записей")
        logger.info(f"   • user_access: {db.execute(text('SELECT COUNT(*) FROM user_access')).scalar()} пользователей")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при очистке: {e}")
        db.rollback()
        raise
    finally:
        db.close()


def clear_only_referrals():
    """Очищает только таблицы приглашений (сохраняет привязку Telegram)"""
    
    db = SessionLocal()
    
    try:
        logger.info("=" * 50)
        logger.info("Очищаем ТОЛЬКО таблицы приглашений")
        logger.info("=" * 50)
        
        # 1. Очищаем referral_rewards
        rewards_count = db.execute(text("SELECT COUNT(*) FROM referral_rewards")).scalar()
        db.execute(text("TRUNCATE TABLE referral_rewards RESTART IDENTITY CASCADE"))
        logger.info(f"✅ Удалено {rewards_count} записей из referral_rewards")
        
        # 2. Очищаем referrals
        referrals_count = db.execute(text("SELECT COUNT(*) FROM referrals")).scalar()
        db.execute(text("TRUNCATE TABLE referrals RESTART IDENTITY CASCADE"))
        logger.info(f"✅ Удалено {referrals_count} записей из referrals")
        
        # 3. Очищаем pending_invites
        pending_count = db.execute(text("SELECT COUNT(*) FROM pending_invites")).scalar()
        db.execute(text("TRUNCATE TABLE pending_invites RESTART IDENTITY CASCADE"))
        logger.info(f"✅ Удалено {pending_count} записей из pending_invites")
        
        db.commit()
        
        logger.info("=" * 50)
        logger.info("✅ Очистка завершена! Привязка Telegram сохранена.")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("ОЧИСТКА РЕФЕРАЛЬНЫХ ДАННЫХ")
    print("=" * 60)
    print("\nВыберите режим очистки:")
    print("1. Полная очистка (все реферальные данные + привязка Telegram)")
    print("2. Частичная очистка (только приглашения, привязка Telegram сохраняется)")
    print("3. Выход")
    
    choice = input("\nВаш выбор (1/2/3): ").strip()
    
    if choice == "1":
        confirm = input("\n⚠️ Вы уверены? Будут удалены ВСЕ реферальные данные и привязка Telegram! (yes/no): ")
        if confirm.lower() == "yes":
            clear_referral_data()
        else:
            print("❌ Отменено")
    elif choice == "2":
        confirm = input("\n⚠️ Вы уверены? Будут удалены только приглашения, награды и ожидания. Привязка Telegram сохранится. (yes/no): ")
        if confirm.lower() == "yes":
            clear_only_referrals()
        else:
            print("❌ Отменено")
    else:
        print("❌ Выход")