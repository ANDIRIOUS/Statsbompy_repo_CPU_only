"""
Real-time Inference Utilities
Helper functions for making predictions during streaming
"""

import os
import joblib
import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple


class MatchPredictor:
    """Real-time match outcome predictor"""

    def __init__(self, model_path: str, scaler_path: str, feature_names_path: str = None):
        """
        Initialize predictor with trained model

        Args:
            model_path: Path to trained model file
            scaler_path: Path to scaler file
            feature_names_path: Path to feature names file (optional)
        """
        self.model = None
        self.scaler = None
        self.feature_columns = None

        if os.path.exists(model_path) and os.path.exists(scaler_path):
            self.load_model(model_path, scaler_path, feature_names_path)

    def load_model(self, model_path: str, scaler_path: str, feature_names_path: str = None):
        """
        Load trained model and scaler

        Args:
            model_path: Path to model file
            scaler_path: Path to scaler file
            feature_names_path: Path to feature names file (optional)
        """
        try:
            self.model = joblib.load(model_path)
            self.scaler = joblib.load(scaler_path)
            print(f"[SUCCESS] Model loaded from {model_path}")
            print(f"[SUCCESS] Scaler loaded from {scaler_path}")

            # Load feature names if available
            if feature_names_path and os.path.exists(feature_names_path):
                self.feature_columns = joblib.load(feature_names_path)
                print(f"[SUCCESS] Feature names loaded from {feature_names_path}")
            elif hasattr(self.scaler, 'feature_names_in_'):
                self.feature_columns = list(self.scaler.feature_names_in_)
                print(f"[INFO] Feature names extracted from scaler")
            else:
                # Default feature order
                self.feature_columns = [
                    'home_possession_count', 'home_shots', 'home_xg',
                    'home_passes', 'home_completed_passes', 'home_pass_completion',
                    'home_duels', 'home_tackles',
                    'away_possession_count', 'away_shots', 'away_xg',
                    'away_passes', 'away_completed_passes', 'away_pass_completion',
                    'away_duels', 'away_tackles'
                ]
                print(f"[INFO] Using default feature order")
        except Exception as e:
            print(f"[ERROR] Failed to load model: {e}")
            raise

    def extract_features_from_aggregates(
        self,
        possession_df: pd.DataFrame,
        xg_df: pd.DataFrame,
        pass_df: pd.DataFrame
    ) -> Optional[pd.DataFrame]:
        """
        Extract features from aggregated statistics

        Args:
            possession_df: Possession statistics
            xg_df: xG statistics
            pass_df: Pass completion statistics

        Returns:
            DataFrame with features for prediction
        """
        try:
            # Get latest window for each team
            teams = possession_df['team_name'].unique()

            if len(teams) < 2:
                return None

            home_team = teams[0]
            away_team = teams[1]

            features = {}

            # Possession features
            for team, prefix in [(home_team, 'home'), (away_team, 'away')]:
                poss_data = possession_df[possession_df['team_name'] == team]
                if not poss_data.empty:
                    features[f'{prefix}_possession_count'] = poss_data['possession_percentage'].iloc[-1]
                else:
                    features[f'{prefix}_possession_count'] = 0

            # xG features
            for team, prefix in [(home_team, 'home'), (away_team, 'away')]:
                xg_data = xg_df[xg_df['team_name'] == team]
                if not xg_data.empty:
                    features[f'{prefix}_xg'] = xg_data['total_xg'].iloc[-1]
                    features[f'{prefix}_shots'] = xg_data['shots_count'].iloc[-1]
                else:
                    features[f'{prefix}_xg'] = 0
                    features[f'{prefix}_shots'] = 0

            # Pass features
            for team, prefix in [(home_team, 'home'), (away_team, 'away')]:
                pass_data = pass_df[pass_df['team_name'] == team]
                if not pass_data.empty:
                    features[f'{prefix}_pass_completion'] = pass_data['pass_completion_pct'].iloc[-1] / 100
                    features[f'{prefix}_completed_passes'] = pass_data['completed_passes'].iloc[-1]
                    features[f'{prefix}_passes'] = pass_data['total_passes'].iloc[-1]
                else:
                    features[f'{prefix}_pass_completion'] = 0
                    features[f'{prefix}_completed_passes'] = 0
                    features[f'{prefix}_passes'] = 0

            # Add default values for features not in streaming data
            features['home_duels'] = 0
            features['away_duels'] = 0
            features['home_tackles'] = 0
            features['away_tackles'] = 0

            return pd.DataFrame([features])

        except Exception as e:
            print(f"[ERROR] Feature extraction failed: {e}")
            return None

    def predict(self, features_df: pd.DataFrame) -> Tuple[str, Dict[str, float]]:
        """
        Make prediction on features

        Args:
            features_df: DataFrame with match features

        Returns:
            Tuple of (predicted_outcome, probabilities_dict)
        """
        if self.model is None or self.scaler is None:
            raise ValueError("Model not loaded")

        try:
            # Use stored feature columns order or default
            if self.feature_columns is None:
                feature_cols = [
                    'home_possession_count', 'home_shots', 'home_xg',
                    'home_passes', 'home_completed_passes', 'home_pass_completion',
                    'home_duels', 'home_tackles',
                    'away_possession_count', 'away_shots', 'away_xg',
                    'away_passes', 'away_completed_passes', 'away_pass_completion',
                    'away_duels', 'away_tackles'
                ]
            else:
                feature_cols = self.feature_columns

            # Fill missing features with 0
            for col in feature_cols:
                if col not in features_df.columns:
                    features_df[col] = 0

            # IMPORTANT: Select columns in the exact order they were during training
            X = features_df[feature_cols].fillna(0)

            # Scale features
            X_scaled = self.scaler.transform(X)

            # Predict
            prediction = self.model.predict(X_scaled)[0]
            probabilities = self.model.predict_proba(X_scaled)[0]

            # Create probabilities dictionary
            prob_dict = {
                cls: prob
                for cls, prob in zip(self.model.classes_, probabilities)
            }

            return prediction, prob_dict

        except Exception as e:
            print(f"[ERROR] Prediction failed: {e}")
            raise

    def format_prediction(
        self,
        prediction: str,
        probabilities: Dict[str, float],
        home_team: str = "Home",
        away_team: str = "Away"
    ) -> str:
        """
        Format prediction for display

        Args:
            prediction: Predicted outcome
            probabilities: Probability dictionary
            home_team: Home team name
            away_team: Away team name

        Returns:
            Formatted prediction string
        """
        outcome_map = {
            'home_win': f'{home_team} Win',
            'away_win': f'{away_team} Win',
            'draw': 'Draw'
        }

        result = f"\n{'='*60}\n"
        result += "MATCH PREDICTION\n"
        result += f"{'='*60}\n\n"
        result += f"Predicted Outcome: {outcome_map.get(prediction, prediction)}\n\n"
        result += "Probabilities:\n"

        for outcome, prob in probabilities.items():
            display_name = outcome_map.get(outcome, outcome)
            bar_length = int(prob * 40)
            bar = '█' * bar_length + '░' * (40 - bar_length)
            result += f"  {display_name:15s} [{bar}] {prob:.1%}\n"

        result += f"\n{'='*60}\n"

        return result


def create_predictor(
    model_path: str = "/app/data/models/modelo.joblib",
    scaler_path: str = "/app/data/models/scaler.joblib",
    feature_names_path: str = "/app/data/models/feature_names.joblib"
) -> Optional[MatchPredictor]:
    """
    Factory function to create predictor instance

    Args:
        model_path: Path to model file
        scaler_path: Path to scaler file
        feature_names_path: Path to feature names file

    Returns:
        MatchPredictor instance or None if model not found
    """
    if os.path.exists(model_path) and os.path.exists(scaler_path):
        try:
            return MatchPredictor(model_path, scaler_path, feature_names_path)
        except Exception as e:
            print(f"[ERROR] Failed to create predictor: {e}")
            return None
    else:
        print(f"[INFO] Model files not found at {model_path}")
        return None
