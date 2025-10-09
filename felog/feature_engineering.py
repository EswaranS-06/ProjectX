import pandas as pd
import re
import math
import os
from collections import Counter
from typing import List
import logging
from datetime import datetime


class FeatureEngineering:
    """
    Consumes normalized logs and returns ML-ready features.
    """

    def __init__(self, df_parsed: pd.DataFrame, window_seconds=60, enable_logging=True):
        self.df = df_parsed
        self.window = window_seconds
        self.enable_logging = enable_logging
        if self.enable_logging:
            self._setup_logging()

    def _setup_logging(self):
        """Set up logging configuration."""
        # Only set up logging if tmp directory exists
        if not os.path.exists("tmp"):
            self.enable_logging = False
            return
            
        log_filename = f"tmp/feature_engineering_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        try:
            logging.basicConfig(
                filename=log_filename,
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                force=True  # Overwrite any existing configuration
            )
            self.logger = logging.getLogger(__name__)
            # Test if we can write to the log file
            self.logger.info("FeatureEngineering initialized")
        except Exception as e:
            print(f"Warning: Could not set up logging to {log_filename}: {e}")
            self.enable_logging = False

    def _log_info(self, message):
        """Log info message if logging is enabled."""
        if self.enable_logging:
            try:
                self.logger.info(message)
            except Exception:
                pass  # Silently fail if logging fails
        print(message)  # Also print to console

    def _log_warning(self, message):
        """Log warning message if logging is enabled."""
        if self.enable_logging:
            try:
                self.logger.warning(message)
            except Exception:
                pass  # Silently fail if logging fails
        print(f"WARNING: {message}")  # Also print to console

    def _calculate_entropy(self, messages: List[str]) -> float:
        """Calculate entropy of tokens in messages."""
        all_tokens = []
        for msg in messages:
            all_tokens.extend(re.findall(r'\w+', msg.lower()))
        if not all_tokens:
            return 0.0
        counts = Counter(all_tokens)
        total = sum(counts.values())
        probs = [c/total for c in counts.values()]
        entropy = -sum(p * math.log2(p) for p in probs if p > 0)
        return entropy

    # ---------- Return Pandas features ----------
    def get_features(self) -> pd.DataFrame:
        """Generate features from parsed logs using time windows."""
        self._log_info(f"Generating features from DataFrame with {len(self.df)} rows")
        
        # Remove rows with missing timestamps
        df = self.df.dropna(subset=['timestamp']).sort_values('timestamp')
        self._log_info(f"Rows with valid timestamps: {len(df)}/{len(self.df)} ({100*len(df)/len(self.df):.1f}%)")
        
        if df.empty:
            # Return empty DataFrame with expected columns
            self._log_warning("No valid timestamps found. Returning empty DataFrame.")
            return pd.DataFrame(columns=[
                'window_start', 'window_end', 'event_count', 'unique_messages',
                'distinct_hosts', 'distinct_processes', 'avg_msg_length',
                'failed_auth_count', 'invalid_user_count', 'entropy_tokens'
            ])
        
        start = df['timestamp'].min()
        end = df['timestamp'].max()
        delta = pd.Timedelta(seconds=self.window)
        self._log_info(f"Time range: {start} to {end}, Window size: {self.window} seconds")
        
        # Create time windows
        windows = []
        current = start
        while current <= end:
            windows.append((current, current + delta))
            current += delta

        self._log_info(f"Created {len(windows)} time windows")
        
        feature_rows = []
        empty_windows = 0
        processed_windows = 0
        
        for i, (w_start, w_end) in enumerate(windows):
            win_df = df[(df['timestamp'] >= w_start) & (df['timestamp'] < w_end)]
            if win_df.empty:
                empty_windows += 1
                continue
            
            processed_windows += 1
            # Calculate features for this window
            features = {
                'window_start': w_start,
                'window_end': w_end,
                'event_count': len(win_df),
                'unique_messages': win_df['message'].nunique(),
                'distinct_hosts': win_df['host'].nunique(),
                'distinct_processes': win_df['process'].nunique(),
                'avg_msg_length': win_df['message'].str.len().mean() if not win_df['message'].empty else 0,
                'failed_auth_count': win_df['message'].str.contains(r'failed password', case=False, na=False).sum(),
                'invalid_user_count': win_df['message'].str.contains(r'invalid user', case=False, na=False).sum(),
                'entropy_tokens': self._calculate_entropy(win_df['message'].tolist())
            }
            feature_rows.append(features)
            
            # Log progress for every 10th window
            if (i + 1) % 10 == 0:
                self._log_info(f"Processed {i + 1} windows, {processed_windows} contained data")
        
        result_df = pd.DataFrame(feature_rows)
        self._log_info(f"Feature generation complete. Processed {len(windows)} windows: {processed_windows} with data, {empty_windows} empty")
        self._log_info(f"Generated {len(result_df)} feature rows with {len(result_df.columns)} columns")
        
        # Log some statistics about the features
        if not result_df.empty:
            self._log_info(f"Feature statistics:")
            self._log_info(f"  - Event count range: {result_df['event_count'].min()} to {result_df['event_count'].max()}")
            self._log_info(f"  - Avg message length range: {result_df['avg_msg_length'].min():.2f} to {result_df['avg_msg_length'].max():.2f}")
            self._log_info(f"  - Entropy range: {result_df['entropy_tokens'].min():.2f} to {result_df['entropy_tokens'].max():.2f}")
        
        return result_df

    # ---------- Save features ----------
    def save_csv(self, file_path: str):
        """Save features to CSV file."""
        self._log_info(f"Saving features to CSV: {file_path}")
        df_feat = self.get_features()
        df_feat.to_csv(file_path, index=False)
        self._log_info(f"Successfully saved {len(df_feat)} rows to {file_path}")

    def save_json(self, file_path: str):
        """Save features to JSON file."""
        self._log_info(f"Saving features to JSON: {file_path}")
        df_feat = self.get_features()
        df_feat.to_json(file_path, orient='records', date_format='iso')
        self._log_info(f"Successfully saved {len(df_feat)} rows to {file_path}")
