from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import requests
import re
import os
from dotenv import load_dotenv

# Telegram 
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

def escape_markdown_v2(text: str) -> str:
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)

def get_chat_id(bot_token: str) -> str:
    url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    results = data.get("result")
    if not results:
        raise ValueError("No updates found. Write a message to your bot first.")
    return str(results[-1]["message"]["chat"]["id"])

def send_telegram_message(bot_token: str, chat_id: str, message: str):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": escape_markdown_v2(message),
        "parse_mode": "MarkdownV2"
    }
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    logging.info("Telegram message sent successfully.")



with DAG(
    dag_id="BitBot_dag",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
):

    # Task: Tabelle erzeugen
    @task
    def create_table():
        try:
            hook = PostgresHook(postgres_conn_id="postgres_default")
            sql = """
            CREATE TABLE IF NOT EXISTS bitcoin_data (
                id SERIAL PRIMARY KEY,
                time TIMESTAMP WITH TIME ZONE NOT NULL,
                value FLOAT NOT NULL
            );
            """
            hook.run(sql)
            logging.info("bitcoin_data table created.")
        except Exception as e:
            chat_id = get_chat_id(TELEGRAM_BOT_TOKEN)
            send_telegram_message(
                TELEGRAM_BOT_TOKEN, chat_id, f"ERROR in create_table:\n{e}"
            )
            raise

    # Task: Backfill (12h, alle 5 min) beim Neustart
    @task
    def backfill_last_12_hours():
        try:
            hook = PostgresHook(postgres_conn_id="postgres_default")

            # Prüfen ob Tabelle leer ist
            count = hook.get_first("SELECT COUNT(*) FROM bitcoin_data;")[0]
            if count > 0:
                logging.info("Table is not empty → skipping backfill.")
                return "SKIPPED"

            logging.info("Table empty → running 12h backfill...")

            # 12h historische Daten holen
            url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
            params = {"vs_currency": "usd", "days": "0.5"}  # 12h = 0.5 Tage

            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            price_data = resp.json()["prices"] 

            # 5-Minuten-Schritte sampeln
            now = datetime.utcnow()
            start = now - timedelta(hours=12)

            target_times = []
            t = start
            while t <= now:
                target_times.append(t)
                t += timedelta(minutes=5)

            # Den jeweils nächsten Preispunkt verwenden
            insert_rows = []
            idx = 0

            for target_time in target_times:
                target_ts_ms = int(target_time.timestamp() * 1000)

                # Suche den ersten Datensatz nach target_ts
                while idx < len(price_data) and price_data[idx][0] < target_ts_ms:
                    idx += 1
                if idx >= len(price_data):
                    break

                price = float(price_data[idx][1])
                insert_rows.append((target_time, price))

            # Batch insert
            logging.info(f"Inserting {len(insert_rows)} backfill rows...")
            for ts, price in insert_rows:
                hook.run(
                    "INSERT INTO bitcoin_data (time, value) VALUES (%s, %s);",
                    parameters=(ts, price)
                )

            logging.info("Backfill completed.")
            return "DONE"

        except Exception as e:
            chat_id = get_chat_id(TELEGRAM_BOT_TOKEN)
            send_telegram_message(
                TELEGRAM_BOT_TOKEN, chat_id, f"ERROR in backfill:\n{e}"
            )
            raise

    # Task: Bitcoin-Preis holen
    @task
    def get_bitcoin_price():
        try:    
            url = "https://api.coingecko.com/api/v3/simple/price"
            params = {"ids": "bitcoin", "vs_currencies": "usd"}

            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            price = resp.json()["bitcoin"]["usd"]

            logging.info(f"Fetched BTC price: {price}")
            return price
        except Exception as e:
            chat_id = get_chat_id(TELEGRAM_BOT_TOKEN)
            send_telegram_message(
                TELEGRAM_BOT_TOKEN, chat_id, f"ERROR in get_bitcoin_price:\n{e}"
            )
            raise

    # Task: Preis in DB speichern
    @task
    def insert_data(price: float):
        try:
            hook = PostgresHook(postgres_conn_id="postgres_default")
            sql = "INSERT INTO bitcoin_data (time, value) VALUES (NOW(), %s);"
            hook.run(sql, parameters=(price,))
            logging.info(f"Inserted BTC price {price}")
        except Exception as e:
            chat_id = get_chat_id(TELEGRAM_BOT_TOKEN)
            send_telegram_message(
                TELEGRAM_BOT_TOKEN, chat_id, f"ERROR in insert_data:\n{e}"
            )
            raise
        
    # DAG Reihenfolge
    create_table() >> backfill_last_12_hours() >> insert_data(get_bitcoin_price())
