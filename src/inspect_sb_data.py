from statsbombpy import sb
import pandas as pd

# La Liga 2020/2021
matches = sb.matches(competition_id=11, season_id=90)
match_id = matches.iloc[0]['match_id']
events = sb.events(match_id=match_id)

print("Columns:", events.columns.tolist())
if 'shot_statsbomb_xg' in events.columns:
    print("shot_statsbomb_xg exists")
    print(events['shot_statsbomb_xg'].head())
else:
    print("shot_statsbomb_xg DOES NOT exist")
    # Check for 'shot' column
    if 'shot' in events.columns:
        print("shot column exists")
        print(events['shot'].head())
