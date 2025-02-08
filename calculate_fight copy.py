import sqlite3
import pandas as pd
import random
from datetime import datetime


def calculate_fight_outcome(db_path="data/ufc_fighters.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT name, weight_class FROM fighters ORDER BY id DESC LIMIT 1")  
        fighter1_data_row = cursor.fetchone()

        if fighter1_data_row:
            fighter1_name = fighter1_data_row[0]
            fighter1_weight_class = fighter1_data_row[1]

            print(f"The most recently added fighter is: {fighter1_name}")
            print(f"Fighter 1: {fighter1_name} is in the {fighter1_weight_class} weight class.")

            cursor.execute("SELECT name FROM fighters WHERE weight_class = ? AND name != ?", (fighter1_weight_class, fighter1_name))
            fighters_in_same_class = cursor.fetchall()

            if not fighters_in_same_class:
                print(f"No other fighters found in the {fighter1_weight_class} weight class.") 
                return

            fighter2_name = random.choice(fighters_in_same_class)[0] 
            print(f"Fighter 2: {fighter2_name} is also in the {fighter1_weight_class} weight class.") 

            # Get full data for fighter 1 and fighter 2 after weight class assignment
            cursor.execute("SELECT * FROM fighters WHERE name=?", (fighter1_name,))
            fighter1 = cursor.fetchone()
            cursor.execute("SELECT * FROM fighters WHERE name=?", (fighter2_name,))
            fighter2 = cursor.fetchone()

            fighter1_data = dict(zip([description[0] for description in cursor.description], fighter1))
            fighter2_data = dict(zip([description[0] for description in cursor.description], fighter2))

            # Basic Fight Simulation 
            fighter1_score = 0
            fighter2_score = 0

            # Striking
            if (fighter1_data['significant_strikes_landed_per_minute'] is None or 
                fighter1_data['significant_striking_accuracy'] is None or 
                fighter2_data['significant_strikes_landed_per_minute'] is None or 
                fighter2_data['significant_striking_accuracy'] is None):
                fighter1_score += 0
                fighter2_score += 0
            else:
                fighter1_score += fighter1_data['significant_strikes_landed_per_minute'] * fighter1_data['significant_striking_accuracy']
                fighter2_score += fighter2_data['significant_strikes_landed_per_minute'] * fighter2_data['significant_striking_accuracy']

            # Takedowns
            if (fighter1_data['average_takedowns_landed_per_15_minutes'] is None or 
                fighter1_data['takedown_accuracy'] is None or 
                fighter2_data['average_takedowns_landed_per_15_minutes'] is None or 
                fighter2_data['takedown_accuracy'] is None):
                fighter1_score += 0
                fighter2_score += 0
            else:
                fighter1_score += fighter1_data['average_takedowns_landed_per_15_minutes'] * fighter1_data['takedown_accuracy']
                fighter2_score += fighter2_data['average_takedowns_landed_per_15_minutes'] * fighter2_data['takedown_accuracy']

            # Submissions
            if (fighter1_data['average_submissions_attempted_per_15_minutes'] is None or
                fighter2_data['average_submissions_attempted_per_15_minutes'] is None):
                fighter1_score += 0
                fighter2_score += 0
            else:
                fighter1_score += fighter1_data['average_submissions_attempted_per_15_minutes']
                fighter2_score += fighter2_data['average_submissions_attempted_per_15_minutes']

            # Height/Reach Advantage 
            if fighter1_data['height_cm'] is None or fighter2_data['height_cm'] is None or fighter1_data['reach_in_cm'] is None or fighter2_data['reach_in_cm'] is None:
                fighter1_score += 0
                fighter2_score += 0
            else:
                height_diff = fighter1_data['height_cm'] - fighter2_data['height_cm']
                reach_diff = fighter1_data['reach_in_cm'] - fighter2_data['reach_in_cm']
                if height_diff > 5:  fighter1_score += 1 
                elif height_diff < -5: fighter2_score += 1
                if reach_diff > 5: fighter1_score += 1
                elif reach_diff < -5: fighter2_score += 1

            # Add some randomness to simulate upsets
            fighter1_score += random.uniform(-2, 2)
            fighter2_score += random.uniform(-2, 2)

            # Determine Winner
            if fighter1_score > fighter2_score:
                winner = fighter1_name
            elif fighter2_score > fighter1_score:
                winner = fighter2_name
            else: # very close fight, add a tie breaker
                if random.random() < 0.5: 
                    winner = fighter1_name
                else:
                    winner = fighter2_name

            print(f"Fight Simulation: {fighter1_name} vs {fighter2_name}")
            print(f"{fighter1_name}: Score = {fighter1_score:.2f}")
            print(f"{fighter2_name}: Score = {fighter2_score:.2f}")
            print(f"Winner: {winner}")

    finally:
        conn.close()

if __name__ == "__main__":
    calculate_fight_outcome()