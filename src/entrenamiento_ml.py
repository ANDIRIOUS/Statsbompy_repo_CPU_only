"""
Machine Learning Training Script for Match Outcome Prediction
Trains a model to predict match results (Home Win / Draw / Away Win)
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import joblib
from statsbombpy import sb


class MatchOutcomePredictor:
    """Train ML model to predict match outcomes"""

    def __init__(
        self,
        data_path: str = "/app/data/processed",
        model_output_path: str = "/app/data/models",
        model_name: str = "modelo.joblib"
    ):
        """
        Initialize ML trainer

        Args:
            data_path: Path to processed data (Parquet files)
            model_output_path: Path to save trained model
            model_name: Name of the model file
        """
        self.data_path = Path(data_path)
        self.model_output_path = Path(model_output_path)
        self.model_name = model_name
        self.model = None
        self.scaler = None

    def load_statsbomb_data(self, num_matches: int = 50) -> pd.DataFrame:
        """
        Load historical match data from Statsbomb

        Args:
            num_matches: Number of matches to load

        Returns:
            DataFrame with match data
        """
        print("[INFO] Loading historical match data from Statsbomb...")

        # Get La Liga competitions
        competitions = sb.competitions()
        laliga = competitions[
            (competitions['competition_name'] == 'La Liga') &
            (competitions['season_name'].str.contains('2020|2021|2022'))
        ]

        if laliga.empty:
            print("[WARNING] No La Liga data found, using available competition...")
            laliga = competitions.head(1)

        all_matches = []

        for _, comp in laliga.iterrows():
            comp_id = int(comp['competition_id'])
            season_id = int(comp['season_id'])

            matches = sb.matches(competition_id=comp_id, season_id=season_id)
            all_matches.append(matches)

            if len(pd.concat(all_matches)) >= num_matches:
                break

        matches_df = pd.concat(all_matches, ignore_index=True).head(num_matches)
        print(f"[INFO] Loaded {len(matches_df)} matches")

        return matches_df

    def extract_match_features(self, match_id: int) -> dict:
        """
        Extract features from a match for ML training

        Args:
            match_id: Match ID

        Returns:
            Dictionary with match features
        """
        try:
            events = sb.events(match_id=match_id)

            # Get unique teams
            teams = events['team'].apply(lambda x: x.get('name') if isinstance(x, dict) else None).unique()
            teams = [t for t in teams if t is not None]

            if len(teams) < 2:
                return None

            home_team = teams[0]
            away_team = teams[1]

            # Filter first half events (period 1)
            first_half = events[events['period'] == 1]

            # Initialize features
            features = {
                'match_id': match_id,
                'home_team': home_team,
                'away_team': away_team
            }

            # Calculate features for each team
            for team, prefix in [(home_team, 'home'), (away_team, 'away')]:
                team_events = first_half[
                    first_half['team'].apply(lambda x: x.get('name') == team if isinstance(x, dict) else False)
                ]

                # Possession count
                features[f'{prefix}_possession_count'] = len(team_events)

                # Shots
                shots = team_events[team_events['type'].apply(
                    lambda x: x.get('name') == 'Shot' if isinstance(x, dict) else False
                )]
                features[f'{prefix}_shots'] = len(shots)

                # Expected Goals (xG)
                xg_values = shots['shot'].apply(
                    lambda x: x.get('statsbomb_xg') if isinstance(x, dict) else 0
                )
                features[f'{prefix}_xg'] = xg_values.sum() if len(xg_values) > 0 else 0

                # Passes
                passes = team_events[team_events['type'].apply(
                    lambda x: x.get('name') == 'Pass' if isinstance(x, dict) else False
                )]
                features[f'{prefix}_passes'] = len(passes)

                # Completed passes
                completed_passes = passes[passes['pass'].apply(
                    lambda x: 'outcome' not in x if isinstance(x, dict) else False
                )]
                features[f'{prefix}_completed_passes'] = len(completed_passes)

                # Pass completion rate
                if len(passes) > 0:
                    features[f'{prefix}_pass_completion'] = len(completed_passes) / len(passes)
                else:
                    features[f'{prefix}_pass_completion'] = 0

                # Duels
                duels = team_events[team_events['type'].apply(
                    lambda x: x.get('name') == 'Duel' if isinstance(x, dict) else False
                )]
                features[f'{prefix}_duels'] = len(duels)

                # Tackles
                tackles = team_events[team_events['type'].apply(
                    lambda x: x.get('name') == 'Tackle' if isinstance(x, dict) else False
                )]
                features[f'{prefix}_tackles'] = len(tackles)

            # Get final score from full match events
            final_events = events[events['period'] == events['period'].max()]

            home_score = 0
            away_score = 0

            for _, event in final_events.iterrows():
                if isinstance(event.get('team'), dict):
                    team_name = event['team'].get('name')
                    if isinstance(event.get('type'), dict) and event['type'].get('name') == 'Shot':
                        if isinstance(event.get('shot'), dict) and event['shot'].get('outcome', {}).get('name') == 'Goal':
                            if team_name == home_team:
                                home_score += 1
                            elif team_name == away_team:
                                away_score += 1

            features['home_score'] = home_score
            features['away_score'] = away_score

            # Determine outcome
            if home_score > away_score:
                features['outcome'] = 'home_win'
            elif away_score > home_score:
                features['outcome'] = 'away_win'
            else:
                features['outcome'] = 'draw'

            return features

        except Exception as e:
            print(f"[ERROR] Failed to extract features for match {match_id}: {e}")
            return None

    def prepare_training_data(self, num_matches: int = 50) -> pd.DataFrame:
        """
        Prepare training dataset from historical matches

        Args:
            num_matches: Number of matches to process

        Returns:
            DataFrame with features and labels
        """
        print(f"[INFO] Preparing training data from {num_matches} matches...")

        matches = self.load_statsbomb_data(num_matches)

        features_list = []

        for idx, match in matches.iterrows():
            match_id = int(match['match_id'])
            print(f"[PROGRESS] Processing match {idx + 1}/{len(matches)}: {match_id}")

            features = self.extract_match_features(match_id)

            if features:
                features_list.append(features)

        training_df = pd.DataFrame(features_list)
        print(f"[SUCCESS] Prepared {len(training_df)} match records")

        return training_df

    def train_model(self, training_df: pd.DataFrame, model_type: str = 'random_forest'):
        """
        Train ML model

        Args:
            training_df: DataFrame with features and labels
            model_type: Type of model ('random_forest' or 'logistic_regression')
        """
        print(f"[INFO] Training {model_type} model...")

        # Select feature columns
        feature_cols = [col for col in training_df.columns if col not in
                       ['match_id', 'home_team', 'away_team', 'home_score', 'away_score', 'outcome']]

        X = training_df[feature_cols]
        y = training_df['outcome']

        # Handle missing values
        X = X.fillna(0)

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        # Scale features
        self.scaler = StandardScaler()
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)

        # Train model
        if model_type == 'random_forest':
            self.model = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            )
        else:
            self.model = LogisticRegression(
                max_iter=1000,
                random_state=42,
                multi_class='multinomial'
            )

        print(f"[INFO] Training on {len(X_train)} samples...")
        self.model.fit(X_train_scaled, y_train)

        # Evaluate
        y_pred = self.model.predict(X_test_scaled)

        print("\n" + "="*60)
        print("MODEL EVALUATION")
        print("="*60)

        print(f"\nAccuracy: {accuracy_score(y_test, y_pred):.3f}")

        print("\nClassification Report:")
        print(classification_report(y_test, y_pred))

        print("\nConfusion Matrix:")
        print(confusion_matrix(y_test, y_pred))

        # Cross-validation
        cv_scores = cross_val_score(self.model, X_train_scaled, y_train, cv=5)
        print(f"\nCross-validation scores: {cv_scores}")
        print(f"Mean CV score: {cv_scores.mean():.3f} (+/- {cv_scores.std() * 2:.3f})")

        # Feature importance (for Random Forest)
        if model_type == 'random_forest':
            feature_importance = pd.DataFrame({
                'feature': feature_cols,
                'importance': self.model.feature_importances_
            }).sort_values('importance', ascending=False)

            print("\nTop 10 Most Important Features:")
            print(feature_importance.head(10))

        print("="*60 + "\n")

    def save_model(self):
        """Save trained model and scaler"""
        self.model_output_path.mkdir(parents=True, exist_ok=True)

        model_path = self.model_output_path / self.model_name
        scaler_path = self.model_output_path / "scaler.joblib"

        # Save model
        joblib.dump(self.model, model_path)
        print(f"[SUCCESS] Model saved to {model_path}")

        # Save scaler
        joblib.dump(self.scaler, scaler_path)
        print(f"[SUCCESS] Scaler saved to {scaler_path}")

        # Save metadata
        metadata = {
            'timestamp': datetime.now().isoformat(),
            'model_type': type(self.model).__name__,
            'classes': list(self.model.classes_)
        }

        metadata_path = self.model_output_path / "model_metadata.joblib"
        joblib.dump(metadata, metadata_path)
        print(f"[SUCCESS] Metadata saved to {metadata_path}")


def main():
    """Main execution function"""
    # Configuration
    data_path = os.getenv('DATA_PROCESSED_PATH', '/app/data/processed')
    model_output_path = os.getenv('MODELS_PATH', '/app/data/models')
    num_matches = int(os.getenv('NUM_TRAINING_MATCHES', '50'))
    model_type = os.getenv('MODEL_TYPE', 'random_forest')

    # Create trainer
    trainer = MatchOutcomePredictor(
        data_path=data_path,
        model_output_path=model_output_path
    )

    try:
        # Prepare training data
        training_df = trainer.prepare_training_data(num_matches=num_matches)

        # Save training data
        training_data_path = Path(data_path) / "training_data.parquet"
        training_data_path.parent.mkdir(parents=True, exist_ok=True)
        training_df.to_parquet(training_data_path)
        print(f"[INFO] Training data saved to {training_data_path}")

        # Train model
        trainer.train_model(training_df, model_type=model_type)

        # Save model
        trainer.save_model()

        print("\n[SUCCESS] Model training completed successfully!")

    except Exception as e:
        print(f"[ERROR] Training failed: {e}")
        raise


if __name__ == "__main__":
    main()
