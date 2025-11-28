from statsbombpy import sb
import pandas as pd
import json

# Fetch competitions to find La Liga
# comps = sb.competitions()
# print(comps[comps['country_name'] == 'Spain'])

# Let's just pick a known match or fetch matches from a recent La Liga season
# La Liga ID is 11. Season 2020/2021 is 90.
matches = sb.matches(competition_id=11, season_id=90)
if not matches.empty:
    match_id = matches.iloc[0]['match_id']
    print(f"Fetching events for match_id: {match_id}")
    
    events = sb.events(match_id=match_id)
    print("Columns:", events.columns.tolist())
    
    # Convert first event to JSON to see structure for Spark Schema
    first_event = events.iloc[0].to_dict()
    # Handle non-serializable types if any (like timestamps usually need string conversion)
    print("First Event JSON:")
    print(json.dumps(first_event, default=str, indent=2))
else:
    print("No matches found.")
