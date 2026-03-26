{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 25,
   "id": "5b1dc802-061b-4dcf-b678-89d6138969b5",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Current working directory: C:\\Users\\AB\\Documents\\projects\n"
     ]
    }
   ],
   "source": [
    "import os\n",
    "import time\n",
    "import logging\n",
    "import pandas as pd\n",
    "from sqlalchemy import create_engine\n",
    "import os\n",
    "# Set this to your project folder where 'data/' and 'inventory.db' are\n",
    "os.chdir(r\"C:\\Users\\AB\\Documents\\projects\")\n",
    "print(\"Current working directory:\", os.getcwd())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 26,
   "id": "82627674-ae3a-427d-9c99-7f47f10e5ea6",
   "metadata": {},
   "outputs": [],
   "source": [
    "os.makedirs(\"logs\", exist_ok=True)\n",
    "os.makedirs(\"data\", exist_ok=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 27,
   "id": "cf4c38ca-2e3b-4ead-855d-6c567f4f00bb",
   "metadata": {},
   "outputs": [],
   "source": [
    "logging.basicConfig(\n",
    "    filename=\"logs/ingestion_db.log\",\n",
    "    level=logging.INFO,\n",
    "    format=\"%(asctime)s - %(levelname)s - %(message)s\",\n",
    "    filemode=\"a\"\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 28,
   "id": "53c3996e-038c-4443-84b1-b3985e6b6f78",
   "metadata": {},
   "outputs": [],
   "source": [
    "engine = create_engine(\"sqlite:///inventory.db\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 29,
   "id": "e5af4cb7-35c1-4096-8eb1-ee8da9cf42b4",
   "metadata": {},
   "outputs": [],
   "source": [
    "def ingest_db(df, table_name, engine):\n",
    "    df.to_sql(table_name, engine, if_exists=\"replace\", index=False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 30,
   "id": "58b9107f-ffcb-415b-94f8-8755b55ef02e",
   "metadata": {},
   "outputs": [],
   "source": [
    "def load_raw_data():\n",
    "    import time, logging, pandas as pd, os\n",
    "    from sqlalchemy import create_engine\n",
    "\n",
    "    start = time.time()\n",
    "    \n",
    "    # Connect to database\n",
    "    engine = create_engine(\"sqlite:///inventory.db\")\n",
    "\n",
    "    for file in os.listdir(\"data\"):\n",
    "        if file.endswith(\".csv\"):\n",
    "            df = pd.read_csv(\"data/\" + file)\n",
    "            logging.info(f\"Ingesting {file} into database\")\n",
    "            print(f\"Ingesting {file}...\")  # <- shows progress in notebook\n",
    "            df.to_sql(file[:-4], engine, if_exists=\"replace\", index=False)\n",
    "\n",
    "    end = time.time()\n",
    "    total_minutes = (end - start) / 60\n",
    "    logging.info(f\"Ingestion completed in {total_minutes:.2f} minutes\")\n",
    "    print(f\"Ingestion completed in {total_minutes:.2f} minutes\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 23,
   "id": "76fb4305-2c37-4099-ab16-15a2fa41df1b",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[]"
      ]
     },
     "execution_count": 23,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "import sqlite3\n",
    "\n",
    "conn = sqlite3.connect(\"inventory.db\")\n",
    "cursor = conn.cursor()\n",
    "\n",
    "cursor.execute(\"SELECT name FROM sqlite_master WHERE type='table';\")\n",
    "tables = cursor.fetchall()\n",
    "\n",
    "tables"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 31,
   "id": "9a191fb0-d3f2-4f91-90ac-a90122bf2483",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "['.ipynb_checkpoints', 'begin_inventory.csv', 'desktop.ini', 'end_inventory.csv', 'purchases.csv', 'purchase_prices.csv', 'sales.csv', 'vendor_invoice.csv']\n"
     ]
    }
   ],
   "source": [
    "import os\n",
    "print(os.listdir(\"data\"))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 32,
   "id": "5d8989e3-9d0b-403a-a91e-06cfaea98d81",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Ingesting begin_inventory.csv...\n",
      "Ingesting end_inventory.csv...\n",
      "Ingesting purchases.csv...\n",
      "Ingesting purchase_prices.csv...\n",
      "Ingesting sales.csv...\n",
      "Ingesting vendor_invoice.csv...\n",
      "Ingestion completed in 42.34 minutes\n"
     ]
    }
   ],
   "source": [
    "from sqlalchemy import create_engine\n",
    "import pandas as pd\n",
    "import logging\n",
    "import time\n",
    "\n",
    "os.makedirs(\"logs\", exist_ok=True)\n",
    "\n",
    "logging.basicConfig(\n",
    "    filename=\"logs/ingestion_db.log\",\n",
    "    level=logging.INFO,\n",
    "    format=\"%(asctime)s - %(levelname)s - %(message)s\",\n",
    "    filemode=\"a\"\n",
    ")\n",
    "\n",
    "engine = create_engine(\"sqlite:///inventory.db\")\n",
    "\n",
    "def load_raw_data():\n",
    "    start = time.time()\n",
    "    for file in os.listdir(\"data\"):\n",
    "        if file.endswith(\".csv\"):\n",
    "            df = pd.read_csv(\"data/\" + file)\n",
    "            logging.info(f\"Ingesting {file} into database\")\n",
    "            print(f\"Ingesting {file}...\")\n",
    "            df.to_sql(file[:-4], engine, if_exists=\"replace\", index=False)\n",
    "    end = time.time()\n",
    "    total_minutes = (end - start) / 60\n",
    "    logging.info(f\"Ingestion completed in {total_minutes:.2f} minutes\")\n",
    "    print(f\"Ingestion completed in {total_minutes:.2f} minutes\")\n",
    "\n",
    "# Call it\n",
    "load_raw_data()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "025abcf9-01fd-4fbd-9a76-7501384b3f34",
   "metadata": {},
   "outputs": [],
   "source": [
    " "
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
   "version": "3.13.5"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
