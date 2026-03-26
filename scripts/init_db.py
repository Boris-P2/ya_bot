{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "f0fb6fc3",
   "metadata": {},
   "outputs": [],
   "source": [
    "import sys\n",
    "import os\n",
    "sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))\n",
    "\n",
    "from database.models import Base\n",
    "from database.session import engine\n",
    "import logging\n",
    "\n",
    "logging.basicConfig(level=logging.INFO)\n",
    "logger = logging.getLogger(__name__)\n",
    "\n",
    "if __name__ == \"__main__\":\n",
    "    logger.info(\"Creating database tables...\")\n",
    "    Base.metadata.create_all(bind=engine)\n",
    "    logger.info(\"Database initialization completed!\")"
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
