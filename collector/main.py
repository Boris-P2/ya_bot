import logging
import time
from typing import Dict, List, Optional
from datetime import datetime
from sqlalchemy.orm import Session

from collector.yandex_client import YandexTaxiClient
from database import crud, models
from database.session import SessionLocal
from shared.config import settings

logger = logging.getLogger(__name__)


class DataCollector:
    """Сборщик данных из Яндекс.Такси"""
    
    def __init__(self):
        self.client = YandexTaxiClient(
            api_key=settings.YA_API_KEY,
            client_id=settings.YA_CLIENT_ID,
            park_id=settings.YA_PARK_ID
        )
        self.days_new_driver_threshold = settings.DAYS_NEW_DRIVER_THRESHOLD
        self.days_transactions_back = settings.DAYS_TRANSACTIONS_BACK
        
    def update_drivers_list(self, db: Session, api_drivers: List[Dict]) -> Dict:
        """Обновляет список водителей"""
        now_utc = datetime.utcnow()
        new_count = 0
        status_updated = 0
        new_driver_ids = []
        
        for api_driver in api_drivers:
            driver_profile = api_driver.get('driver_profile', {})
            account = api_driver.get('account', {}) or api_driver.get('accounts', [{}])[0] if api_driver.get('accounts') else {}
            current_status = api_driver.get('current_status', {})
            
            driver_id = driver_profile.get('id', '')
            if not driver_id:
                continue
            
            work_status = driver_profile.get('work_status', '')
            existing = crud.get_driver(db, driver_id)
            
            driver_data = {
                'driver_id': driver_id,
                'first_name': driver_profile.get('first_name', ''),
                'last_name': driver_profile.get('last_name', ''),
                'created_date': driver_profile.get('created_date', ''),
                'work_status': work_status,
                'balance': account.get('balance', ''),
                'currency': account.get('currency', ''),
                'current_status': current_status.get('status', ''),
                'last_transaction_date': account.get('last_transaction_date', '')
            }
            
            if not existing:
                # Новый водитель
                driver_data['last_status_updated'] = now_utc
                crud.save_driver(db, driver_data)
                new_count += 1
                new_driver_ids.append(driver_id)
                logger.info(f"New driver added: {driver_data['first_name']} {driver_data['last_name']}")
            else:
                # Существующий водитель
                needs_update = False
                
                if existing.work_status != work_status:
                    needs_update = True
                elif existing.last_status_updated:
                    days_since_update = (now_utc - existing.last_status_updated).days
                    if work_status == 'working' and days_since_update >= settings.DAYS_STATUS_UPDATE_WORKING:
                        needs_update = True
                    elif work_status == 'not_working' and days_since_update >= settings.DAYS_STATUS_UPDATE_NOT_WORKING:
                        needs_update = True
                
                if needs_update:
                    driver_data['last_status_updated'] = now_utc
                    crud.save_driver(db, driver_data)
                    status_updated += 1
                    logger.info(f"Status updated for {driver_data['first_name']}: {work_status}")
        
        return {'new': new_count, 'updated': status_updated, 'new_driver_ids': new_driver_ids}
    
    def update_orders_for_drivers(self, db: Session, drivers: List[models.Driver]) -> Dict:
        """Обновляет количество заказов для переданных водителей"""
        updated_count = 0
        errors = []
        
        for driver in drivers:
            logger.info(f"Updating orders for {driver.first_name} {driver.last_name}")
            
            result = self.client.get_driver_transactions(
                driver.driver_id,
                self.days_transactions_back
            )
            
            if result['success']:
                new_orders = result['orders_count']
                
                # Обновляем только если новое значение >= старого
                if new_orders >= driver.orders_count:
                    crud.update_driver_orders(db, driver.driver_id, new_orders)
                    updated_count += 1
                    logger.info(f"  Orders: {driver.orders_count} -> {new_orders}")
                else:
                    error_msg = f"Orders decreased for {driver.driver_id}: {driver.orders_count} -> {new_orders}"
                    logger.warning(error_msg)
                    errors.append(error_msg)
            else:
                error_msg = f"API error for {driver.driver_id}: {result.get('error')}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        return {'updated': updated_count, 'errors': errors}
    
    def update_all_driver_phones(self, batch_size: int = 2000) -> Dict:
        """
        Обновляет номера телефонов для всех водителей
        batch_size - количество водителей за один запуск (чтобы не превысить лимиты API)
        """
        db = SessionLocal()
        updated = 0
        errors = []
        
        try:
            # Получаем водителей, у которых еще нет номера телефона
            drivers = db.query(models.Driver).filter(
                models.Driver.phone.is_(None)  # только те, у кого нет телефона
            ).limit(batch_size).all()
            
            logger.info(f"Updating phones for {len(drivers)} drivers...")
            
            for driver in drivers:
                phone = self.client.get_driver_phone(driver.driver_id)
                
                if phone:
                    driver.phone = phone
                    db.commit()
                    updated += 1
                    logger.info(f"Phone updated for {driver.first_name} {driver.last_name}: {phone}")
                else:
                    # Если телефон не получен, отмечаем пустой строкой, чтобы не запрашивать снова
                    driver.phone = ''
                    db.commit()
                    logger.warning(f"No phone found for {driver.first_name} {driver.last_name}")
                
                # Не превышаем лимиты API
                time.sleep(0.5)
            
            return {'updated': updated, 'errors': errors}
            
        except Exception as e:
            logger.error(f"Failed to update phones: {e}")
            return {'updated': updated, 'errors': [str(e)]}
        finally:
            db.close()
    
    def run_full_update(self) -> Dict:
        """
        Запускает полный цикл обновления с FIFO очередью
        """
        start_time = datetime.utcnow()
        db = SessionLocal()
        
        try:
            logger.info("Starting full data collection...")
            
            # Шаг 1: Загружаем водителей из API
            api_drivers = self.client.fetch_all_drivers()
            if not api_drivers:
                raise Exception("No drivers fetched from API")
            
            # Шаг 2: Обновляем список водителей
            drivers_result = self.update_drivers_list(db, api_drivers)
            
            # Шаг 3: Получаем следующих водителей из очереди (FIFO)
            from database.crud import get_next_drivers_for_update, update_queue_timestamp
            
            queue_entries = get_next_drivers_for_update(db, batch_size=500)
            
            orders_result = {'updated': 0, 'errors': []}
            
            if queue_entries:
                # Получаем объекты водителей из БД
                driver_ids = [entry.driver_id for entry in queue_entries]
                drivers_to_update = db.query(models.Driver).filter(
                    models.Driver.driver_id.in_(driver_ids)
                ).all()
                
                # Обновляем заказы
                orders_result = self.update_orders_for_drivers(db, drivers_to_update)
                
                # Обновляем время в очереди
                for driver in drivers_to_update:
                    update_queue_timestamp(db, driver.driver_id)
                
                logger.info(f"Updated orders for {orders_result['updated']} drivers from queue")
            else:
                logger.info("No drivers in update queue")
            
            # Шаг 4: Сохраняем лог
            crud.create_collection_log(
                db,
                status='success',
                new_drivers_added=drivers_result['new'],
                status_updated=drivers_result['updated'],
                orders_updated=orders_result['updated'],
                api_calls_used=len(api_drivers) // 500 + 1 + len(queue_entries),
                errors=orders_result['errors']
            )
            
            return {
                'success': True,
                'new_drivers': drivers_result['new'],
                'status_updates': drivers_result['updated'],
                'orders_updated': orders_result['updated'],
                'errors': orders_result['errors']
            }
            
        except Exception as e:
            logger.error(f"Collection failed: {e}")
            crud.create_collection_log(
                db,
                status='failed',
                error_message=str(e)
            )
            return {
                'success': False,
                'error': str(e)
            }
        finally:
            db.close()