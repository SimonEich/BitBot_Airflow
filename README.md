BitBot_airflow Dokumentation

FHNW - Simon Eich

Problemstellung

Es gibt verschiedene Modele mit welchen man Kursverläufe vorherzusagen kann. Ich möchte möglichst aktuelle Daten sammeln um die Möglichkeit zu haben verschiedene Modelle zu testen. Es ist sehr aufwendig und Fehleranfällig das Sammeln der Daten von Hand zu machen. Eine gute möglichkeit den Prozess zu automatisiern ist 
die Verwendung von Airflow. 

Da ich den Prozess nicht überwachen möchte, aber möglichst schnell bei einem Problem eingreifen um keine Daten zu verpassen, muss eine Benachrichtigungsfunktion integriert werden. 



Zielarchitektur (Soll-Zustand)

Die gewünschte Lösung soll den Bitcoin-Preis automatisch in regelmäßigen Intervallen erfassen, in einer Datenbank speichern und bei Fehlern sofort eine Telegram-Benachrichtigung senden.

Das System besteht aus folgenden Komponenten:

Apache Airflow
Orchestriert den gesamten Prozess
Führt Tasks sequentiell und planbar aus
Stellt automatisches Logging und Monitoring bereit

CoinGecko API
Liefert aktuelle Bitcoin-Preise im 5-Minuten-Intervall
Vergange Werte um Modele zu trainieren

PostgreSQL Datenbank
Speichert die Preisdaten
Ermöglicht spätere Analysen oder Reporting

Telegram Bot
Sendet Benachrichtigungen bei Fehlern
Ermöglicht unmittelbare Kontrolle der Pipeline


Ablaufdiagramm

create_table() >> backfill_last_12_hours() >> insert_data(get_bitcoin_price())



Technologie-Entscheidung(en)

Warum Apache Airflow?
Airflow eignet sich hervorragend für ETL-Prozesse, API-Abfragen und datengetriebene Workflows. Relevante Vorteile:
Wiederholbare und planbare Ausführung (Cron-ähnliche Zeitsteuerung)
Native Verbindung zu PostgreSQL über PostgresHook
Einfache Skalierbarkeit
Für diese Anwendung ist Airflow wesentlich geeigneter als Apache Beam, da die Aufgabe nicht datenstrom-basiert, sondern zeitgesteuert und orchestriert ist.

Warum Docker Compose?
Das Airflow-Setup wurde bewusst mit Docker Compose realisiert. Gründe:
Einfache Erweiterbarkeit (z. B. weitere Services, Worker)
Industrie-Standard für lokale und produktionsnahe Umgebungen

Warum PostgreSQL?
stabile, relationale Datenbank
Airflow bringt fertige Hooks und Operatoren mit
leicht weiterverwendbar für Dashboards oder Analysen

Warum Telegram als Benachrichtigungssystem?
einfache Bot-Anbindung
kostenlos und zuverlässig
ermöglicht Monitoring von unterwegs


Umsetzung

Der umgesetzte Airflow-DAG besteht aus vier Haupttasks:

(1) create_table()

Erstellt (falls noch nicht vorhanden) die Tabelle bitcoin_data.
Dies stellt sicher, dass der DAG auf leeren Systemen ohne Setup funktioniert („idempotent“).

(2) backfill_last_12_hour()
Füllt die Datenbank mit Daten der letzten zwölf Stunden.


(3) get_bitcoin_price()
Fragt den aktuellen Bitcoin-Preis über die CoinGecko-API ab und gibt ihn an die folgenden Tasks weiter.

(4) insert_data(price)
Speichert den Preis in der PostgreSQL-Datenbank.

Telegram-Error-Handling
Jeder Task ist in einen try/except-Block gekapselt.
Bei Fehlern wird automatisch eine Telegram-Nachricht verschickt.

Dadurch wird sofort sichtbar, wenn:
die API nicht erreichbar ist
die Datenbank nicht arbeitet
Airflow ein Problem hat

