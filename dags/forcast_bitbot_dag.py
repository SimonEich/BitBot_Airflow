from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging
import requests
import re
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import os
from dotenv import load_dotenv

# Telegram 
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

def escape_markdown_v2(text: str) -> str:
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)

def get_chat_id(bot_token: str) -> str:
    url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("result")
    if not results:
        raise ValueError("Keine Updates für den Bot gefunden. Schreibe zuerst an deinen Bot.")
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
    logging.info("Telegram message sent.")

# ---------------------------
# DAG: stündliche Prognose
# ---------------------------
with DAG(
    dag_id="BitBot_forecast_dag",
    start_date=datetime(2024, 1, 1),
    schedule="0 * * * *",  # jede Stunde zur vollen Stunde
    catchup=False,
):

    @task
    def create_forecast_table():
        """Erstellt die Tabelle bitcoin_forecast, falls sie nicht existiert."""
        try:
            hook = PostgresHook(postgres_conn_id="postgres_default")
            sql = """
            CREATE TABLE IF NOT EXISTS bitcoin_forecast (
                id SERIAL PRIMARY KEY,
                time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                predicted_value DOUBLE PRECISION NOT NULL,
                actual_value DOUBLE PRECISION
            );
            """
            hook.run(sql)
            logging.info("Tabelle bitcoin_forecast vorhanden/erstellt.")
        except Exception as e:
            try:
                chat_id = get_chat_id(TELEGRAM_BOT_TOKEN)
                send_telegram_message(TELEGRAM_BOT_TOKEN, chat_id, f"❌ ERROR in create_forecast_table:\n{e}")
            except Exception:
                logging.exception("Fehler beim Senden der Telegram-Fehlermeldung.")
            raise

    @task
    def load_bitcoin_data():
        """
        Lädt Werte aus bitcoin_data.
        Gibt None zurück, wenn zu wenige Daten existieren.
        """
        try:
            hook = PostgresHook(postgres_conn_id="postgres_default")
            rows = hook.get_records("SELECT time, value FROM bitcoin_data ORDER BY time ASC;")

            if not rows or len(rows) < 10:
                logging.warning("⚠️ Zu wenige Daten (<10). Forecast wird übersprungen.")
                return None

            records = []
            for t, v in rows:
                records.append({"time": str(t), "value": float(v)})
            return records

        except Exception as e:
            try:
                chat_id = get_chat_id(TELEGRAM_BOT_TOKEN)
                send_telegram_message(TELEGRAM_BOT_TOKEN, chat_id, f"❌ ERROR in load_bitcoin_data:\n{e}")
            except:
                logging.exception("Fehler beim Senden der Telegram-Fehlermeldung.")
            raise


    @task
    def forecast_next_hour(records):
        if records is None:
            logging.warning("⚠️ forecast_next_hour wird übersprungen (zu wenige Daten).")
            return None

        try:
            df = pd.DataFrame(records)
            df["time"] = pd.to_datetime(df["time"])
            df = df.sort_values("time").reset_index(drop=True)

            df["idx"] = np.arange(len(df))
            X = df[["idx"]].values
            y = df["value"].values

            model = LinearRegression()
            model.fit(X, y)

            next_idx = int(df["idx"].max()) + 1
            prediction = float(model.predict(np.array([[next_idx]]))[0])
            logging.info(f"Prediction (next hour): {prediction}")
            return prediction

        except Exception as e:
            try:
                chat_id = get_chat_id(TELEGRAM_BOT_TOKEN)
                send_telegram_message(TELEGRAM_BOT_TOKEN, chat_id, f"❌ ERROR in forecast_next_hour:\n{e}")
            except:
                logging.exception("Fehler beim Senden der Telegram-Fehlermeldung.")
            raise


    @task
    def store_forecast(predicted_value):
        if predicted_value is None:
            logging.info("⏭️ store_forecast übersprungen (keine Vorhersage).")
            return "skipped"

        try:
            hook = PostgresHook(postgres_conn_id="postgres_default")
            sql = "INSERT INTO bitcoin_forecast (predicted_value) VALUES (%s);"
            hook.run(sql, parameters=(predicted_value,))
            logging.info("Forecast gespeichert.")
        except Exception as e:
            try:
                chat_id = get_chat_id(TELEGRAM_BOT_TOKEN)
                send_telegram_message(TELEGRAM_BOT_TOKEN, chat_id, f"❌ ERROR in store_forecast:\n{e}")
            except:
                logging.exception("Fehler beim Senden der Telegram-Fehlermeldung.")
            raise


    @task
    def update_previous_forecast():
        """
        Aktualisiert die älteste Forecast-Zeile mit actual_value IS NULL
        mit dem aktuellsten Wert aus bitcoin_data (letzter bekannter Wert).
        """
        try:
            hook = PostgresHook(postgres_conn_id="postgres_default")
            last = hook.get_first("SELECT value FROM bitcoin_data ORDER BY time DESC LIMIT 1;")
            if not last:
                logging.info("Kein letzter Wert in bitcoin_data vorhanden; update skipped.")
                return "no_actual"
            last_value = float(last[0])

            update_sql = """
            UPDATE bitcoin_forecast
            SET actual_value = %s
            WHERE id = (
                SELECT id FROM bitcoin_forecast
                WHERE actual_value IS NULL
                ORDER BY time ASC
                LIMIT 1
            );
            """
            hook.run(update_sql, parameters=(last_value,))
            logging.info("Vorherige Prognose mit actual_value aktualisiert.")
            return "updated"
        except Exception as e:
            try:
                chat_id = get_chat_id(TELEGRAM_BOT_TOKEN)
                send_telegram_message(TELEGRAM_BOT_TOKEN, chat_id, f"❌ ERROR in update_previous_forecast:\n{e}")
            except Exception:
                logging.exception("Fehler beim Senden der Telegram-Fehlermeldung.")
            raise

    @task
    def send_forecast_to_telegram(predicted_value):
        if predicted_value is None:
            logging.info("⏭️ Telegram-Sendung übersprungen (keine Vorhersage).")
            return

        try:
            chat_id = get_chat_id(TELEGRAM_BOT_TOKEN)
            msg = (
                f"📈 *Bitcoin Prognose*\n\n"
                f"Vorhersage für die nächste Stunde:\n"
                f"*{predicted_value:,.2f} USD*"
            )
            send_telegram_message(TELEGRAM_BOT_TOKEN, chat_id, msg)
        except Exception as e:
            logging.exception(e)


    @task
    def accuracy_report():
        """
        Bewertet, wie gut die Vorhersagen waren.
        Fokus: Fälle, bei denen predicted_value > actual_value (Kaufempfehlung).
        """
        try:
            hook = PostgresHook(postgres_conn_id="postgres_default")

            # Alle Forecasts laden, bei denen ein actual_value existiert
            rows = hook.get_records("""
                SELECT predicted_value, actual_value
                FROM bitcoin_forecast
                WHERE actual_value IS NOT NULL
                ORDER BY time ASC;
            """)

            if not rows or len(rows) < 5:
                logging.info("Zu wenige Daten für ein Accuracy-Reporting.")
                return "too_few"

            total = 0
            signals = 0
            correct = 0

            for predicted, actual in rows:
                total += 1

                # Kauf-Signal
                if predicted > actual:
                    signals += 1

                    # War das Signal korrekt?
                    # (aktueller Preis ist höher als zum Zeitpunkt der Vorhersage)
                    if actual > actual:  # das hier wird unten korrigiert
                        correct += 1

            # --- Bugfix: richtiger Vergleich ---
            # actual_value ist der echte Wert NACH der Stunde
            # predicted_value basiert auf dem letzten realen Wert DAVOR
            # Wir brauchen also nochmals den vorherigen realen Wert!
            # Holen wir aus der DB:

            rows2 = hook.get_records("""
                SELECT
                    f.predicted_value,
                    f.actual_value,
                    (SELECT value
                     FROM bitcoin_data
                     WHERE time < f.time
                     ORDER BY time DESC
                     LIMIT 1) AS actual_before
                FROM bitcoin_forecast f
                WHERE f.actual_value IS NOT NULL
                ORDER BY f.time ASC;
            """)

            signals = 0
            correct = 0

            for predicted, actual_after, actual_before in rows2:
                if actual_before is None:
                    continue

                # Signal: Preis soll steigen → predicted > actual_before
                if predicted > actual_before:
                    signals += 1

                    # Korrekt, wenn tatsächlicher Preis danach steigt
                    if actual_after > actual_before:
                        correct += 1

            if signals == 0:
                accuracy = 0
            else:
                accuracy = (correct / signals) * 100

            # Telegram Nachricht
            chat_id = get_chat_id(TELEGRAM_BOT_TOKEN)
            msg = (
                "📊 *Genauigkeitsbericht* für Kaufempfehlungen\n\n"
                f"🔁 Gesamt geprüfte Zeiträume: *{len(rows2)}*\n"
                f"💡 Kaufempfehlungen: *{signals}*\n"
                f"✅ Davon korrekt: *{correct}*\n\n"
                f"🎯 *Treffsicherheit: {accuracy:.2f}%*\n\n"
                "Interpretation:\n"
                "- Hohe Quote → Modell sagt gute Kaufsignale\n"
                "- Niedrige Quote → Modell verbessern"
            )
            send_telegram_message(TELEGRAM_BOT_TOKEN, chat_id, msg)

            return accuracy

        except Exception as e:
            logging.exception("Fehler in accuracy_report")
            try:
                chat_id = get_chat_id(TELEGRAM_BOT_TOKEN)
                send_telegram_message(
                    TELEGRAM_BOT_TOKEN,
                    chat_id,
                    f"❌ ERROR in accuracy_report:\n{e}"
                )
            except Exception:
                pass
            raise



    # ---------------------------
    # DAG Flow
    # ---------------------------
    create_forecast_table() >> update_previous_forecast()

    records = load_bitcoin_data()
    prediction = forecast_next_hour(records)

    store = store_forecast(prediction)
    send = send_forecast_to_telegram(prediction)
    accuracy = accuracy_report()
    
    store >> send >> accuracy
