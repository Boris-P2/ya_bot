{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "01ff49ad",
   "metadata": {},
   "outputs": [],
   "source": [
    "import sys\n",
    "import os\n",
    "sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))\n",
    "\n",
    "from bot.main import run_bot\n",
    "\n",
    "if __name__ == \"__main__\":\n",
    "    print(\"🤖 Starting Telegram Bot...\")\n",
    "    run_bot()"
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
