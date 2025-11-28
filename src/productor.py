import time
import json
import random
from kafka import KafkaProducer
from statsbombpy import sb
import pandas as pd

# Configuration
KAFKA_TOPIC = 'statsbomb-eventos'
KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'  # Internal Docker network address

def get_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Kafka!")
            return producer
        except Exception as e:
            print(f"Waiting for Kafka... ({e})")
            time.sleep(5)

def fetch_matches():
    # La Liga (Spain) - Competition ID 11
    # Season 2020/2021 - Season ID 90
    print("Fetching matches for La Liga 2020/2021...")
    matches = sb.matches(competition_id=11, season_id=90)
    return matches

def process_match(producer, match_id):
    print(f"Fetching events for match {match_id}...")
    events = sb.events(match_id=match_id)
    
    # Sort by timestamp/index to simulate real flow
    events = events.sort_values('index')
    
    print(f"Sending {len(events)} events to Kafka...")
    
    for _, event in events.iterrows():
        # Convert to dict and handle serialization
        event_dict = event.to_dict()
        
        # Ensure all fields are JSON serializable
        for k, v in event_dict.items():
            # Handle lists (like location) which cause ValueError in pd.isna
            if isinstance(v, (list, tuple)):
                continue
            if pd.isna(v):
                event_dict[k] = None
            elif hasattr(v, 'isoformat'): # Dates
                event_dict[k] = v.isoformat()
        
        # Add match_id if not present
        event_dict['match_id'] = match_id
        
        producer.send(KAFKA_TOPIC, event_dict)
        
        # Simulate real-time (fast-forwarded)
        # 0.01s sleep is roughly 100 events/sec. 
        # A match has ~3000 events. 30s per match.
        time.sleep(0.05) 
        
    print(f"Finished match {match_id}")

def main():
    producer = get_producer()
    matches = fetch_matches()
    
    # Shuffle matches to keep it interesting or just iterate
    match_ids = matches['match_id'].tolist()
    
    for match_id in match_ids:
        process_match(producer, match_id)
        time.sleep(2) # Pause between matches

if __name__ == "__main__":
    main()
