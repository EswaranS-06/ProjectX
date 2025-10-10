"""
Log feature pipeline that composes `LogParser` and `FeatureEngineering`.
Provides multi-source ingestion (file, folder, UDP) and a single run_pipeline API
that returns a features DataFrame suitable for ML or dashboards.
"""
from typing import List, Optional
from datetime import timedelta
import pandas as pd

from .parser import LogParser
from .feature_engineering import FeatureEngineering


class LogFeaturePipeline:
    """Orchestrates ingestion, parsing, windowing and feature engineering.

    Usage:
        pipeline = LogFeaturePipeline(window_seconds=60)
        pipeline.ingest_from_file('logs/syslog.log')
        features = pipeline.run()
    """

    def __init__(self, window_seconds: int = 60, enable_logging: bool = True):
        self.window_seconds = window_seconds
        self.parser = LogParser(enable_logging=enable_logging)
        self._raw_df: Optional[pd.DataFrame] = None

    # Ingestion helpers -------------------------------------------------
    def ingest_from_file(self, file_path: str):
        """Load logs from a single file into the internal parser."""
        self.parser.from_file(file_path)
        return self

    def ingest_from_folder(self, folder_path: str):
        """Load logs from a folder (all files) into the internal parser."""
        self.parser.from_folder(folder_path)
        return self

    def ingest_from_udp(self, host: str = '0.0.0.0', port: int = 514, max_logs: int = 1000):
        """Listen on a UDP port (syslog) and ingest up to max_logs lines."""
        self.parser.from_udp_port(host=host, port=port, max_logs=max_logs)
        return self

    # Pipeline ----------------------------------------------------------
    def parse(self) -> pd.DataFrame:
        """Run normalization and keep a copy of the parsed DataFrame."""
        df = self.parser.normalize()
        # Ensure timestamp column is parsed to pandas datetime
        if 'timestamp' in df.columns:
            try:
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
            except Exception:
                df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        # Map parser columns to expected feature engineering column names
        # FeatureEngineering expects 'host' and 'process' columns
        if 'service' in df.columns and 'process' not in df.columns:
            df['process'] = df['service']
        if 'source_file' in df.columns and 'host' not in df.columns:
            df['host'] = df['source_file']
        # Ensure message column exists
        if 'message' not in df.columns and 'message_raw' in df.columns:
            df['message'] = df['message_raw']
        self._raw_df = df
        return df

    def run(self) -> pd.DataFrame:
        """Full pipeline: parse logs and compute windowed features."""
        if self._raw_df is None:
            self.parse()
        fe = FeatureEngineering(self._raw_df, window_seconds=self.window_seconds, enable_logging=False)
        return fe.get_features()


# Convenience function
def run_pipeline_from_files(paths: List[str], window_seconds: int = 60) -> pd.DataFrame:
    """Helper that ingests multiple files and returns feature DataFrame."""
    p = LogFeaturePipeline(window_seconds=window_seconds, enable_logging=False)
    for path in paths:
        p.ingest_from_file(path)
    return p.run()
