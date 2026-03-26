{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "12892460",
   "metadata": {},
   "outputs": [],
   "source": [
    "from sqlalchemy import Column, Integer, String, DateTime, Float, JSON, Text, BigInteger\n",
    "from sqlalchemy.ext.declarative import declarative_base\n",
    "from datetime import datetime\n",
    "\n",
    "Base = declarative_base()\n",
    "\n",
    "class Driver(Base):\n",
    "    \"\"\"Модель водителя\"\"\"\n",
    "    __tablename__ = 'drivers'\n",
    "    \n",
    "    id = Column(Integer, primary_key=True, autoincrement=True)\n",
    "    driver_id = Column(String(100), unique=True, index=True, nullable=False)\n",
    "    first_name = Column(String(100))\n",
    "    last_name = Column(String(100))\n",
    "    created_date = Column(String(50))\n",
    "    work_status = Column(String(50), index=True)\n",
    "    balance = Column(String(50))\n",
    "    currency = Column(String(10))\n",
    "    current_status = Column(String(50))\n",
    "    last_transaction_date = Column(String(50))\n",
    "    orders_count = Column(Integer, default=0)\n",
    "    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)\n",
    "    last_status_updated = Column(DateTime, default=datetime.utcnow)\n",
    "    \n",
    "    # Метаданные\n",
    "    priority_score = Column(Integer, default=0)\n",
    "    created_at = Column(DateTime, default=datetime.utcnow)\n",
    "\n",
    "class CollectionLog(Base):\n",
    "    \"\"\"Логирование сборов\"\"\"\n",
    "    __tablename__ = 'collection_logs'\n",
    "    \n",
    "    id = Column(Integer, primary_key=True, autoincrement=True)\n",
    "    started_at = Column(DateTime, default=datetime.utcnow)\n",
    "    finished_at = Column(DateTime)\n",
    "    status = Column(String(20))  # success, failed\n",
    "    new_drivers_added = Column(Integer, default=0)\n",
    "    status_updated = Column(Integer, default=0)\n",
    "    orders_updated = Column(Integer, default=0)\n",
    "    api_calls_used = Column(Integer, default=0)\n",
    "    errors = Column(JSON, default=list)\n",
    "    error_message = Column(Text)\n",
    "\n",
    "class UpdateQueue(Base):\n",
    "    \"\"\"Очередь для приоритетного обновления\"\"\"\n",
    "    __tablename__ = 'update_queue'\n",
    "    \n",
    "    id = Column(Integer, primary_key=True, autoincrement=True)\n",
    "    driver_id = Column(String(100), index=True)\n",
    "    priority_score = Column(Integer, default=0)\n",
    "    queued_at = Column(DateTime, default=datetime.utcnow)\n",
    "    processed_at = Column(DateTime)\n",
    "    status = Column(String(20), default='pending')  # pending, processing, completed, failed"
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
