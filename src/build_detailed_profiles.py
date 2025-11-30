import pandas as pd
import json
import os
from datetime import datetime

print("BUILDING COMPLETE PLAYER PROFILES WITH ALL DATA...")
print("=" * 50)

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Get the project root directory (parent of src/)
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
# Set base path for data
base_path = os.path.join(PROJECT_ROOT, "data", "raw")

def load_data_safely(file_path):
    try:
        return pd.read_csv(file_path, low_memory=False)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return pd.DataFrame()

print("Loading ALL datasets...")
profiles_df = load_data_safely(f"{base_path}/player_profiles/player_profiles.csv")
performances_df = load_data_safely(f"{base_path}/player_performances/player_performances.csv")
transfer_df = load_data_safely(f"{base_path}/transfer_history/transfer_history.csv")
market_value_df = load_data_safely(f"{base_path}/player_market_value/player_market_value.csv")
injuries_df = load_data_safely(f"{base_path}/player_injuries/player_injuries.csv")
national_df = load_data_safely(f"{base_path}/player_national_performances/player_national_performances.csv")
teammates_df = load_data_safely(f"{base_path}/player_teammates_played_with/player_teammates_played_with.csv")

print(f"Datasets loaded:")
print(f"   - Profiles: {len(profiles_df)} players")
print(f"   - Performances: {len(performances_df):,} season records")
print(f"   - Transfers: {len(transfer_df):,} transfer records")
print(f"   - Market Values: {len(market_value_df):,} value records")

def get_transfer_history(player_id, transfer_df):
    """Get complete transfer history"""
    player_transfers = transfer_df[transfer_df['player_id'] == player_id]
    if player_transfers.empty:
        return "No transfer history available."
    
    # Sort by transfer date
    player_transfers = player_transfers.sort_values('transfer_date')
    
    transfer_text = "##Career Transfers\n"
    for _, transfer in player_transfers.iterrows():
        from_team = str(transfer.get('from_team_name', 'Unknown')) if pd.notna(transfer.get('from_team_name')) else 'Unknown'
        to_team = str(transfer.get('to_team_name', 'Unknown')) if pd.notna(transfer.get('to_team_name')) else 'Unknown'
        season = str(transfer.get('season_name', '')) if pd.notna(transfer.get('season_name')) else ''
        fee = transfer.get('transfer_fee', 0) if pd.notna(transfer.get('transfer_fee')) else 0
        
        transfer_text += f"- *{season}*: {from_team} → {to_team}"
        if fee > 0:
            transfer_text += f" (€{fee:,})"
        transfer_date = str(transfer.get('transfer_date', '')) if pd.notna(transfer.get('transfer_date')) else ''
        transfer_text += f" - {transfer_date}\n"
    
    return transfer_text

def get_season_performances(player_id, performances_df):
    """Get season-by-season performance stats"""
    player_performances = performances_df[performances_df['player_id'] == player_id]
    if player_performances.empty:
        return "No performance data available."
    
    # Group by season and team
    season_stats = player_performances.groupby(['season_name', 'team_name']).agg({
        'goals': 'sum',
        'assists': 'sum', 
        'nb_on_pitch': 'sum',
        'minutes_played': 'sum',
        'yellow_cards': 'sum',
        'direct_red_cards': 'sum'
    }).reset_index()
    
    performance_text = "##Season Performance\n"
    for _, season in season_stats.iterrows():
        apps = season.get('nb_on_pitch', 0) if pd.notna(season.get('nb_on_pitch')) else 0
        goals = season.get('goals', 0) if pd.notna(season.get('goals')) else 0
        assists = season.get('assists', 0) if pd.notna(season.get('assists')) else 0
        team = str(season.get('team_name', 'Unknown')) if pd.notna(season.get('team_name')) else 'Unknown'
        
        if apps > 0:
            performance_text += f"- *{season['season_name']}* ({team}): {apps} apps, {goals} goals, {assists} assists"
            if season.get('minutes_played', 0) > 0:
                performance_text += f", {season['minutes_played']:,} minutes"
            performance_text += "\n"
    
    return performance_text

def get_market_value_history(player_id, market_value_df):
    """Get market value progression"""
    player_values = market_value_df[market_value_df['player_id'] == player_id]
    if player_values.empty:
        return "No market value data available."
    
    # Get latest values (most recent first)
    latest_values = player_values.sort_values('date_unix', ascending=False).head(8)
    
    value_text = "##Market Value History\n"
    for _, value in latest_values.iterrows():
        date = str(value.get('date_unix', '')) if pd.notna(value.get('date_unix')) else ''
        market_value = value.get('value', 0) if pd.notna(value.get('value')) else 0
        if market_value > 0 and date:
            value_text += f"- *{date}*: €{market_value:,.0f}\n"
    
    return value_text

def get_injury_history(player_id, injuries_df):
    player_injuries = injuries_df[injuries_df['player_id'] == player_id]
    if player_injuries.empty:
        return "##Injury History\nNo significant injury history recorded."
    
    injury_text = "##Injury History\n"
    for _, injury in player_injuries.head(8).iterrows():
        reason = str(injury.get('injury_reason', 'Unknown')) if pd.notna(injury.get('injury_reason')) else 'Unknown'
        season = str(injury.get('season_name', '')) if pd.notna(injury.get('season_name')) else ''
        days = injury.get('days_missed', 0) if pd.notna(injury.get('days_missed')) else 0
        games = injury.get('games_missed', 0) if pd.notna(injury.get('games_missed')) else 0
        
        injury_text += f"- *{reason}* ({season}): {days} days missed"
        if games > 0:
            injury_text += f", {games} games missed"
        injury_text += "\n"
    
    return injury_text

def get_national_career(player_id, national_df):
    player_national = national_df[national_df['player_id'] == player_id]
    if player_national.empty:
        return "##International Career\nNo national team data available."
    
    nat_career = "##International Career\n"
    for _, nat in player_national.iterrows():
        matches = nat.get('matches', 0)
        goals = nat.get('goals', 0)
        
        if matches > 0:
            nat_career += f"- *Caps: {matches}, **Goals*: {goals}"
            if pd.notna(nat.get('career_state')):
                nat_career += f", *Status*: {nat.get('career_state')}"
            if pd.notna(nat.get('debut')):
                nat_career += f", *Debut*: {nat.get('debut')}"
            nat_career += "\n"
    
    return nat_career

def get_teammates(player_id, teammates_df):
    player_teammates = teammates_df[teammates_df['player_id'] == player_id]
    if player_teammates.empty:
        return "##Notable Teammates\nNo teammate data available."
    
    top_teammates = player_teammates.nlargest(6, 'minutes_played_with')
    teammates_text = "##Notable Teammates\n"
    for _, tm in top_teammates.iterrows():
        teammate_name = str(tm.get('teammate_player_name', 'N/A')) if pd.notna(tm.get('teammate_player_name')) else 'N/A'
        minutes = tm.get('minutes_played_with', 0) if pd.notna(tm.get('minutes_played_with')) else 0
        if minutes > 0:
            hours = minutes // 60
            teammates_text += f"- *{teammate_name}*: {hours:,} hours played together\n"
    
    return teammates_text

def get_player_summary(player_profile, performances_df, player_id):
    """Generate a career summary"""
    player_performances = performances_df[performances_df['player_id'] == player_id]
    
    total_goals = player_performances['goals'].sum()
    total_assists = player_performances['assists'].sum()
    total_apps = player_performances['nb_on_pitch'].sum()
    
    summary = f"##Career Summary\n"
    position = str(player_profile.get('position', 'N/A')) if pd.notna(player_profile.get('position')) else 'N/A'
    citizenship = str(player_profile.get('citizenship', 'N/A')) if pd.notna(player_profile.get('citizenship')) else 'N/A'
    summary += f"- *Position*: {position}\n"
    summary += f"- *Nationality*: {citizenship}\n"
    
    if total_apps > 0:
        summary += f"- *Career Apps*: {total_apps:,}\n"
    if total_goals > 0:
        summary += f"- *Career Goals*: {total_goals:,}\n"
    if total_assists > 0:
        summary += f"- *Career Assists*: {total_assists:,}\n"
    
    current_club = str(player_profile.get('current_club_name', 'N/A')) if pd.notna(player_profile.get('current_club_name')) else 'N/A'
    summary += f"- *Current Club*: {current_club}\n"
    
    return summary

print(f"Creating COMPLETE profiles for {len(profiles_df)} players...")

complete_profiles = []

for idx, player in profiles_df.iterrows():
    player_id = player['player_id']
    
    player_name = str(player.get('player_name', 'Unknown')) if pd.notna(player.get('player_name')) else 'Unknown'
    
    complete_profile = {
        'player_id': int(player_id),
        'player_name': player_name,
        'detailed_content': f"""
# {player_name}

{get_player_summary(player, performances_df, player_id)}

{get_transfer_history(player_id, transfer_df)}

{get_season_performances(player_id, performances_df)}

{get_market_value_history(player_id, market_value_df)}

{get_injury_history(player_id, injuries_df)}

{get_national_career(player_id, national_df)}

{get_teammates(player_id, teammates_df)}

Data sourced from Transfermarkt - Comprehensive football database
        """.strip(),
        'metadata': {
            'position': str(player.get('position', '')) if pd.notna(player.get('position')) else '',
            'nationality': str(player.get('citizenship', '')) if pd.notna(player.get('citizenship')) else '',
            'current_club': str(player.get('current_club_name', '')) if pd.notna(player.get('current_club_name')) else '',
            'birth_date': str(player.get('date_of_birth', '')) if pd.notna(player.get('date_of_birth')) else '',
            'height': str(player.get('height', '')) if pd.notna(player.get('height')) else '',
            'foot': str(player.get('foot', '')) if pd.notna(player.get('foot')) else ''
        }
    }
    
    complete_profiles.append(complete_profile)

    # Progress indicator
    if (idx + 1) % 10000 == 0:
        print(f"Processed {idx + 1} players...")

print(f"Created {len(complete_profiles)} COMPLETE player profiles")

# Save complete profiles
output_file = os.path.join(PROJECT_ROOT, "data", "processed", "complete_player_profiles.json")
os.makedirs(os.path.dirname(output_file), exist_ok=True)
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(complete_profiles, f, ensure_ascii=False, indent=2)

print(f"COMPLETE PROFILES READY!")
print(f"Output file: {output_file}")
print(f"Total profiles: {len(complete_profiles):,}")

# Show a sample of a real player with data
print(f"\nSAMPLE COMPLETE PROFILE:")
print("=" * 60)
sample_profile = complete_profiles[10]  # Get a different player
print(sample_profile['detailed_content'][:1500] + "..." if len(sample_profile['detailed_content']) > 1500 else sample_profile['detailed_content'])