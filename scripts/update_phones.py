import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from collector.main import DataCollector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    print("📞 Updating driver phone numbers...")
    collector = DataCollector()
    result = collector.update_all_driver_phones(batch_size=100)
    print(f"✅ Updated {result['updated']} drivers")