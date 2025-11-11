"""
Kafka Producer for Statsbomb La Liga Events
Simulates real-time streaming of football match events
"""

import json
import time
import os
from datetime import datetime
from typing import List, Dict
from kafka import KafkaProducer
from kafka.errors import KafkaError
import pandas as pd
from statsbombpy import sb


class StatsbombProducer:
    """Producer that streams Statsbomb events to Kafka"""

    def __init__(
        self,
        bootstrap_servers: str = 'kafka:29092',
        topic: str = 'statsbomb-eventos',
        event_delay: float = 0.5
    ):
        """
        Initialize Statsbomb Kafka Producer

        Args:
            bootstrap_servers: Kafka broker address
            topic: Kafka topic name
            event_delay: Delay between events in seconds (to simulate real-time)
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.event_delay = event_delay
        self.producer = None

    def connect(self) -> None:
        """Connect to Kafka broker"""
        print(f"[INFO] Connecting to Kafka at {self.bootstrap_servers}...")

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            print(f"[SUCCESS] Connected to Kafka successfully!")
        except KafkaError as e:
            print(f"[ERROR] Failed to connect to Kafka: {e}")
            raise

    def get_laliga_matches(self, limit: int = None) -> pd.DataFrame:
        """
        Get La Liga matches from Statsbomb

        Args:
            limit: Maximum number of matches to retrieve

        Returns:
            DataFrame with match information
        """
        print("[INFO] Fetching La Liga matches from Statsbomb...")

        # Get competitions
        competitions = sb.competitions()

        # Filter for La Liga
        laliga = competitions[
            (competitions['competition_name'] == 'La Liga') &
            (competitions['season_name'].str.contains('2020|2021|2022'))
        ].head(1)

        if laliga.empty:
            print("[WARNING] No La Liga data found, using available competition...")
            laliga = competitions.head(1)

        competition_id = int(laliga.iloc[0]['competition_id'])
        season_id = int(laliga.iloc[0]['season_id'])

        print(f"[INFO] Competition: {laliga.iloc[0]['competition_name']} - {laliga.iloc[0]['season_name']}")

        # Get matches
        matches = sb.matches(competition_id=competition_id, season_id=season_id)

        if limit:
            matches = matches.head(limit)

        print(f"[INFO] Found {len(matches)} matches")
        return matches

    def get_match_events(self, match_id: int) -> pd.DataFrame:
        """
        Get events for a specific match

        Args:
            match_id: Match ID

        Returns:
            DataFrame with match events
        """
        print(f"[INFO] Fetching events for match {match_id}...")
        events = sb.events(match_id=match_id)
        print(f"[INFO] Retrieved {len(events)} events")
        return events

    def prepare_event(self, event: pd.Series) -> Dict:
        """
        Prepare event data for streaming

        Args:
            event: Event row from DataFrame

        Returns:
            Dictionary with event data
        """
        # Convert pandas Series to dict and handle NaN values
        event_dict = event.to_dict()

        # Clean up NaN values
        for key, value in event_dict.items():
            if pd.isna(value):
                event_dict[key] = None
            elif isinstance(value, (list, dict)):
                event_dict[key] = json.loads(json.dumps(value, default=str))

        # Add metadata
        event_dict['_producer_timestamp'] = datetime.now().isoformat()

        return event_dict

    def send_event(self, event: Dict, match_id: int) -> None:
        """
        Send event to Kafka topic

        Args:
            event: Event data dictionary
            match_id: Match ID (used as partition key)
        """
        try:
            # Use match_id as key for partitioning
            key = f"match_{match_id}"

            future = self.producer.send(
                self.topic,
                key=key,
                value=event
            )

            # Wait for send to complete
            record_metadata = future.get(timeout=10)

            # Log event type and timestamp
            event_type = event.get('type', {}).get('name', 'Unknown')
            minute = event.get('minute', 'N/A')
            second = event.get('second', 'N/A')

            print(f"[SENT] Match {match_id} | {minute}:{second} | {event_type} | "
                  f"Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")

        except KafkaError as e:
            print(f"[ERROR] Failed to send event: {e}")

    def stream_match(self, match_id: int) -> None:
        """
        Stream all events for a specific match

        Args:
            match_id: Match ID to stream
        """
        print(f"\n{'='*60}")
        print(f"[START] Streaming match {match_id}")
        print(f"{'='*60}\n")

        # Get events
        events_df = self.get_match_events(match_id)

        # Sort by timestamp to simulate real-time progression
        events_df = events_df.sort_values(['minute', 'second'])

        # Stream events
        for idx, event in events_df.iterrows():
            event_data = self.prepare_event(event)
            self.send_event(event_data, match_id)

            # Simulate real-time delay
            time.sleep(self.event_delay)

        print(f"\n[COMPLETE] Finished streaming match {match_id}")
        print(f"Total events sent: {len(events_df)}\n")

    def stream_multiple_matches(self, num_matches: int = 5) -> None:
        """
        Stream events from multiple matches

        Args:
            num_matches: Number of matches to stream
        """
        matches = self.get_laliga_matches(limit=num_matches)

        print(f"\n{'='*60}")
        print(f"[INFO] Streaming {len(matches)} matches")
        print(f"{'='*60}\n")

        for idx, match in matches.iterrows():
            match_id = int(match['match_id'])
            home_team = match['home_team']
            away_team = match['away_team']

            print(f"\n[MATCH] {home_team} vs {away_team} (ID: {match_id})")

            try:
                self.stream_match(match_id)
            except Exception as e:
                print(f"[ERROR] Failed to stream match {match_id}: {e}")
                continue

    def close(self) -> None:
        """Close Kafka producer connection"""
        if self.producer:
            print("[INFO] Flushing remaining messages...")
            self.producer.flush()
            print("[INFO] Closing Kafka producer...")
            self.producer.close()
            print("[SUCCESS] Producer closed successfully")


def main():
    """Main execution function"""
    # Configuration from environment or defaults
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    topic = os.getenv('KAFKA_TOPIC', 'statsbomb-eventos')
    event_delay = float(os.getenv('EVENT_DELAY', '0.5'))
    num_matches = int(os.getenv('MATCH_LIMIT', '5'))

    # Create producer
    producer = StatsbombProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        event_delay=event_delay
    )

    try:
        # Connect to Kafka
        producer.connect()

        # Stream matches
        producer.stream_multiple_matches(num_matches=num_matches)

    except KeyboardInterrupt:
        print("\n[INFO] Streaming interrupted by user")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        raise
    finally:
        producer.close()


if __name__ == "__main__":
    main()
