import streamlit as st
import sqlite3
import pandas as pd
import random
import plotly.express as px

# Global Fight Data Variables
fighter1_name = None
fighter2_name = None
winner = None
fighter1_weight_class = None
fight_df = None

def calculate_fight_outcome(db_path="data/ufc_fighters.db"):
    global fighter1_name, fighter2_name, winner, fighter1_weight_class # Globals declared at the beginning of the function
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

            # save fight data and fighter data to a DataFrame as a single row
            global fight_df
            fight_df = pd.DataFrame({
                "fighter1_name": [fighter1_name],
                "fighter2_name": [fighter2_name],
                "fighter1_score": [fighter1_score],
                "fighter2_score": [fighter2_score],
                "winner": [winner],
                "weight_class": [fighter1_weight_class],
                "fighter1_height_cm": [fighter1_data['height_cm']],
                "fighter2_height_cm": [fighter2_data['height_cm']],
                "fighter1_weight_kg": [fighter1_data['weight_in_kg']],
                "fighter2_weight_kg": [fighter2_data['weight_in_kg']],
                "fighter1_reach_cm": [fighter1_data['reach_in_cm']],
                "fighter2_reach_cm": [fighter2_data['reach_in_cm']],
                "fighter1_stance": [fighter1_data['stance']],
                "fighter2_stance": [fighter2_data['stance']],
                "fighter1_significant_strikes_landed_per_minute": [fighter1_data['significant_strikes_landed_per_minute']],
                "fighter2_significant_strikes_landed_per_minute": [fighter2_data['significant_strikes_landed_per_minute']],
                "fighter1_significant_striking_accuracy": [fighter1_data['significant_striking_accuracy']],
                "fighter2_significant_striking_accuracy": [fighter2_data['significant_striking_accuracy']],
                "fighter1_takedown_accuracy": [fighter1_data['takedown_accuracy']],
                "fighter2_takedown_accuracy": [fighter2_data['takedown_accuracy']],
                "fighter1_takedown_defense": [fighter1_data['takedown_defense']],
                "fighter2_takedown_defense": [fighter2_data['takedown_defense']],
                "fighter1_wins": [fighter1_data['wins']],
                "fighter2_wins": [fighter2_data['wins']],
                "fighter1_losses": [fighter1_data['losses']],
                "fighter2_losses": [fighter2_data['losses']],
                "fighter1_draws": [fighter1_data['draws']],
                "fighter2_draws": [fighter2_data['draws']]
            })
        

            print(f"Fight Simulation: {fighter1_name} vs {fighter2_name}")
            print(f"{fighter1_name}: Score = {fighter1_score:.2f}")
            print(f"{fighter2_name}: Score = {fighter2_score:.2f}")
            print(f"Winner: {winner}")


    finally:
        conn.close()

def display_fight_data(): # new function
    col1, col2, col3 = st.columns(3)

    with col1:
        st.header("Fighter 1")
        st.write(f"Fighter 1: {fight_df['fighter1_name'].iloc[0] if fight_df is not None else 'N/A'}") # Handle None values
        st.write(f"Weight Class: {fight_df['weight_class'].iloc[0] if fight_df is not None else 'N/A'}")

    with col2:
        st.header("Fight Outcome")
        st.write(f"Winner: {winner if winner else 'N/A'}") # Handle None values

    with col3:
        st.header("Fighter 2")
        st.write(f"Fighter 2: {fighter2_name if fighter2_name else 'N/A'}") # Handle None values
        st.write(f"Weight Class: {fighter1_weight_class if fighter1_weight_class else 'N/A'}") # Display Fighter 2's weight class


    # Second row
    col4, col5, col6 = st.columns(3)

    with col4:
        if fight_df is not None:
            st.subheader("Fighter Comparison")
            comparison_df = pd.DataFrame({
            "Metric": ["Height", "Height", "Weight", "Weight", "Reach", "Reach"],
            "Fighter": ["Fighter 1", "Fighter 2", "Fighter 1", "Fighter 2", "Fighter 1", "Fighter 2"],
            "Value": [
                fight_df['fighter1_height_cm'].iloc[0], fight_df['fighter2_height_cm'].iloc[0],
                fight_df['fighter1_weight_kg'].iloc[0], fight_df['fighter2_weight_kg'].iloc[0],
                fight_df['fighter1_reach_cm'].iloc[0], fight_df['fighter2_reach_cm'].iloc[0]
            ]
            })

            fig = px.bar(comparison_df, x="Metric", y="Value", color="Fighter", barmode='group',
                 labels={"Metric": "Attributes", "Value": "Values (cm/kg)"})
            st.plotly_chart(fig)

    with col5:
 
        if fight_df is not None:
            st.subheader("Striking and Takedown Comparison")
            striking_takedown_df = pd.DataFrame({
                "Metric": ["Significant Strikes Landed Per Minute", "Significant Strikes Landed Per Minute", 
                           "Significant Striking Accuracy", "Significant Striking Accuracy", 
                           "Takedown Accuracy", "Takedown Accuracy", 
                           "Takedown Defense", "Takedown Defense"],
                "Fighter": ["Fighter 1", "Fighter 2", "Fighter 1", "Fighter 2", 
                            "Fighter 1", "Fighter 2", "Fighter 1", "Fighter 2"],
                "Value": [
                    fight_df['fighter1_significant_strikes_landed_per_minute'].iloc[0], fight_df['fighter2_significant_strikes_landed_per_minute'].iloc[0],
                    fight_df['fighter1_significant_striking_accuracy'].iloc[0], fight_df['fighter2_significant_striking_accuracy'].iloc[0],
                    fight_df['fighter1_takedown_accuracy'].iloc[0], fight_df['fighter2_takedown_accuracy'].iloc[0],
                    fight_df['fighter1_takedown_defense'].iloc[0], fight_df['fighter2_takedown_defense'].iloc[0]
                ]
            })

            fig = px.bar(striking_takedown_df, x="Metric", y="Value", color="Fighter", barmode='group',
                         labels={"Metric": "Attributes", "Value": "Values"})
            st.plotly_chart(fig)
    with col6:
        if fight_df is not None:
            st.subheader("Fighter Records")
            records_df = pd.DataFrame({
            "Record": ["Wins", "Losses", "Draws"],
            "Fighter 1": [fight_df['fighter1_wins'].iloc[0], fight_df['fighter1_losses'].iloc[0], fight_df['fighter1_draws'].iloc[0]],
            "Fighter 2": [fight_df['fighter2_wins'].iloc[0], fight_df['fighter2_losses'].iloc[0], fight_df['fighter2_draws'].iloc[0]]
            })

            fig = px.bar(records_df, x="Record", y=["Fighter 1", "Fighter 2"], barmode='group',
                 labels={"Record": "Record", "value": "Count"})
            st.plotly_chart(fig)

def main():
    st.set_page_config(layout="wide")
    st.title("UFC Fight Simulator")
    st.write("This app simulates a fight between two UFC fighters based on their stats.")

    if st.button("Calculate Fight Outcome", key="calculate_button"):  # Add a unique key here
        calculate_fight_outcome()

    
    if fight_df is not None: # This if block is new
        display_fight_data()

    else: # This else block is new
        st.write("Click the button to calculate the fight outcome.")


if __name__ == "__main__":
    main()