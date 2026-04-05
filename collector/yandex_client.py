import requests
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

class YandexTaxiClient:
    """Клиент для работы с API Яндекс.Такси"""
    
    def __init__(self, api_key: str, client_id: str, park_id: str):
        self.api_key = api_key
        self.client_id = client_id
        self.park_id = park_id
        self.api_url_drivers = "https://fleet-api.taxi.yandex.net/v1/parks/driver-profiles/list"
        self.api_url_transactions = "https://fleet-api.taxi.yandex.net/v2/parks/driver-profiles/transactions/list"

    def get_driver_phone(self, driver_id: str) -> Optional[str]:
        """Получает номер телефона водителя по driver_id"""
        url = "https://fleet-api.taxi.yandex.net/v2/parks/contractors/driver-profile"
        
        headers = {
            "X-API-Key": self.api_key,
            "X-Client-ID": self.client_id,
            "X-Park-ID": self.park_id,
        }
        
        params = {
            "contractor_profile_id": driver_id
        }
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                # Извлекаем телефон из ответа
                phone = data.get('person', {}).get('contact_info', {}).get('phone')
                return phone
            else:
                logger.warning(f"Failed to get phone for {driver_id}: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting phone for {driver_id}: {e}")
            return None

    def fetch_drivers_page(self, offset: int = 0, limit: int = 500) -> Optional[Dict]:
        """Загружает одну страницу водителей"""
        data = {
            "query": {
                "park": {
                    "id": self.park_id
                }
            },
            "limit": min(limit, 1000),
            "offset": offset,
            "fields": {
                "driver_profile": ["id", "first_name", "last_name", "created_date", "work_status"],
                "account": ["last_transaction_date", "balance", "currency"],
                "current_status": ["status"]
            }
        }
        
        headers = {
            "X-API-Key": self.api_key,
            "X-Client-ID": self.client_id,
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(self.api_url_drivers, headers=headers, json=data, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching drivers page: {e}")
            return None
    
    def fetch_all_drivers(self) -> List[Dict]:
        """Загружает всех водителей с пагинацией"""
        all_drivers = []
        offset = 0
        limit = 500
        page = 1
        
        logger.info(f"Fetching all drivers from Yandex API...")
        
        while True:
            logger.debug(f"Fetching page {page} (offset: {offset})")
            result = self.fetch_drivers_page(offset=offset, limit=limit)
            
            if not result:
                break
            
            drivers = result.get('driver_profiles', [])
            total = result.get('total', 0)
            
            if not drivers:
                break
            
            all_drivers.extend(drivers)
            logger.info(f"Fetched {len(drivers)} drivers (total: {len(all_drivers)}/{total})")
            
            if len(drivers) < limit or len(all_drivers) >= total:
                break
                
            offset += limit
            page += 1
            time.sleep(0.5)
        
        logger.info(f"Total drivers fetched: {len(all_drivers)}")
        return all_drivers
    
    def get_driver_transactions(self, driver_id: str, days_back: int = 30) -> Dict[str, Any]:
        """Получает транзакции водителя и возвращает количество уникальных заказов"""
        to_date = datetime.now(timezone.utc)
        from_date = to_date - timedelta(days=days_back)
        
        from_date_str = from_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        to_date_str = to_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        all_transactions = []
        cursor = None
        
        while True:
            payload = {
                "query": {
                    "park": {
                        "id": self.park_id,
                        "driver_profile": {"id": driver_id},
                        "transaction": {"event_at": {"from": from_date_str, "to": to_date_str}}
                    }
                },
                "limit": 1000
            }
            if cursor:
                payload["cursor"] = cursor
            
            headers = {
                "X-API-Key": self.api_key,
                "X-Client-ID": self.client_id,
                "Content-Type": "application/json"
            }
            
            try:
                response = requests.post(self.api_url_transactions, headers=headers, json=payload, timeout=30)
                response.raise_for_status()
                
                result = response.json()
                transactions = result.get('transactions', [])
                if not transactions:
                    break
                
                all_transactions.extend(transactions)
                cursor = result.get('cursor')
                if not cursor:
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching transactions for {driver_id}: {e}")
                return {"success": False, "error": str(e), "orders_count": 0}
        
        # Извлекаем уникальные order_id
        unique_orders = set()
        for t in all_transactions:
            order_id = t.get('order_id')
            if order_id:
                unique_orders.add(order_id)
        
        return {
            "success": True,
            "transactions_count": len(all_transactions),
            "orders_count": len(unique_orders)
        }