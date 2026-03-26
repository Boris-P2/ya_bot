{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "e1b70e26",
   "metadata": {},
   "outputs": [],
   "source": [
    "import sys\n",
    "import os\n",
    "sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))\n",
    "\n",
    "import logging\n",
    "from collector.main import DataCollector\n",
    "from shared.config import settings\n",
    "\n",
    "logging.basicConfig(\n",
    "    level=logging.INFO,\n",
    "    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'\n",
    ")\n",
    "\n",
    "if __name__ == \"__main__\":\n",
    "    print(\"🚀 Starting Yandex Taxi Data Collector...\")\n",
    "    collector = DataCollector()\n",
    "    \n",
    "    # Однократный запуск\n",
    "    result = collector.run_full_update()\n",
    "    print(f\"Collection result: {result}\")"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.4"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
