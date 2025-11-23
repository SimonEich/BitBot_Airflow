from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging
import requests
import re

# ---------------------------------------------------------
# Telegram 
# ---------------------------------------------------------
TELEGRAM_BOT_TOKEN = "8267838711:AAGMwD7BywobC79reVPxJ8Aypi1-ld1XWCk"

def escape_markdown_v2(text: str) -> str:
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)

def get_chat_id(bot_token: str) -> str:
    """Fetch last chat_id from bot updates."""
    url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    results = data.get("result")
    if not results:
        raise ValueError("No updates found. Write a message to your bot first.")

    return str(results[-1]["message"]["chat"]["id"])

def send_telegram_message(bot_token: str, chat_id: str, message: str):
    """Send Telegram message with MarkdownV2 formatting."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": escape_markdown_v2(message),
        "parse_mode": "MarkdownV2"
    }
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    logging.info("Telegram message sent successfully.")


# ---------------------------------------------------------
# Haupt-DAG
# ---------------------------------------------------------
with DAG(
    dag_id="BitBot_dag",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
):

    # ---------------------------
    # Task: Tabelle erzeugen
    # ---------------------------
    @task
    def create_table():
        try:
            hook = PostgresHook(postgres_conn_id="postgres_default")
            sql = """
            CREATE TABLE IF NOT EXISTS bitcoin_data (
                id SERIAL PRIMARY KEY,
                time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                value INTEGER NOT NULL
            );
            """
            hook.run(sql)
            print(">>> bitcoin_data table created.")
        except Exception as e:
            chat_id = get_chat_id(TELEGRAM_BOT_TOKEN)
            send_telegram_message(
                TELEGRAM_BOT_TOKEN,
                chat_id,
                f"❌ ERROR in create_table:\n{e}"
            )
            raise

    # ---------------------------
    # Task: Bitcoin-Preis holen
    # ---------------------------
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
                TELEGRAM_BOT_TOKEN,
                chat_id,
                f"❌ ERROR in get_bitcoin_price:\n{e}"
            )
            raise

    # ---------------------------
    # Task: Preis in DB speichern
    # ---------------------------
    @task
    def insert_data(price: int):
        try:
            hook = PostgresHook(postgres_conn_id="postgres_default")
            sql = f"INSERT INTO bitcoin_data (value) VALUES ({price});"
            hook.run(sql)
            print(f">>> Inserted BTC price {price}")
        except Exception as e:
            chat_id = get_chat_id(TELEGRAM_BOT_TOKEN)
            send_telegram_message(
                TELEGRAM_BOT_TOKEN,
                chat_id,
                f"❌ ERROR in insert_data:\n{e}"
            )
            raise


    # ---------------------------
    # DAG Reihenfolge
    # ---------------------------
    price = get_bitcoin_price()
    create_table() >> insert_data(price)
