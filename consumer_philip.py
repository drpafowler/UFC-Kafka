import json
import sqlite3
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import time

''' example json: {"name": "Danilo Belluardo", "nickname": "Caterpillar", "wins": 12, "losses": 5, "draws": 0, "height_cm": 182.88, "weight_in_kg": 70.31, "reach_in_cm": 187.96, "stance": "Southpaw", "date_of_birth": "1994-07-21", "significant_strikes_landed_per_minute": 1.52, "significant_striking_accuracy": 61.0, "significant_strikes_absorbed_per_minute": 4.79, "significant_strike_defence": 41.0, "average_takedowns_landed_per_15_minutes": 3.5, "takedown_accuracy": 100.0, "takedown_defense": 50.0, "average_submissions_attempted_per_15_minutes": 1.52}'''

KAFKA_TOPIC = 'ufc-fighter'
KAFKA_BROKER = 'localhost:9092'

def sqlite_connect():
    '''Connect to SQLite database'''
    conn = sqlite3.connect('data/ufc_fighters.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fighters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            nickname TEXT,
            wins INTEGER,
            losses INTEGER,
            draws INTEGER,
            height_cm REAL,
            weight_in_kg REAL,
            reach_in_cm REAL,
            stance TEXT,
            date_of_birth TEXT,
            significant_strikes_landed_per_minute REAL,
            significant_striking_accuracy REAL,
            significant_strikes_absorbed_per_minute REAL,
            significant_strike_defence REAL,
            average_takedowns_landed_per_15_minutes REAL,
            takedown_accuracy REAL,
            takedown_defense REAL,
            average_submissions_attempted_per_15_minutes REAL
        )
    ''')
    conn.commit()
    conn.close()

def sqlite_insert(fighter):
    '''Insert fighter into SQLite database'''
    conn = sqlite3.connect('data/ufc_fighters.db')
    cursor = conn.cursor()
    filtered_fighter = {key: fighter[key] for key in (
        'name', 'nickname', 'wins', 'losses', 'draws', 'height_cm', 'weight_in_kg', 'reach_in_cm', 'stance', 'date_of_birth', 
        'significant_strikes_landed_per_minute', 'significant_striking_accuracy', 'significant_strikes_absorbed_per_minute', 
        'significant_strike_defence', 'average_takedowns_landed_per_15_minutes', 'takedown_accuracy', 'takedown_defense', 
        'average_submissions_attempted_per_15_minutes'
    )}
    cursor.execute('''INSERT INTO fighters (
        name, nickname, wins, losses, draws, height_cm, weight_in_kg, reach_in_cm, stance, date_of_birth, 
        significant_strikes_landed_per_minute, significant_striking_accuracy, significant_strikes_absorbed_per_minute, 
        significant_strike_defence, average_takedowns_landed_per_15_minutes, takedown_accuracy, takedown_defense, 
        average_submissions_attempted_per_15_minutes
    ) VALUES (
        :name, :nickname, :wins, :losses, :draws, :height_cm, :weight_in_kg, :reach_in_cm, :stance, :date_of_birth, 
        :significant_strikes_landed_per_minute, :significant_striking_accuracy, :significant_strikes_absorbed_per_minute, 
        :significant_strike_defence, :average_takedowns_landed_per_15_minutes, :takedown_accuracy, :takedown_defense, 
        :average_submissions_attempted_per_15_minutes
    )''', filtered_fighter)
    conn.commit()
    conn.close()

def main():
    '''Consume fighters from Kafka topic and insert into SQLite database'''
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BROKER, value_deserializer=lambda v: json.loads(v.decode('utf-8')))
    for message in consumer:
        fighter = json.loads(message.value)
        sqlite_insert(fighter)
        print(f'Consumed: {fighter}')
        time.sleep(1)

if __name__ == '__main__':
    sqlite_connect()
    main()