import pandas as pd
import re
from dateutil import parser as date_parser
import socket
import os
from typing import List
import logging
from datetime import datetime


class LogParser:
    """
    Handles ingestion and normalization of raw logs.
    """

    def __init__(self, enable_logging=True):
        self.raw_logs = []
        self.enable_logging = enable_logging
        if self.enable_logging:
            self._setup_logging()

    def _setup_logging(self):
        """Set up logging configuration."""
        # Only set up logging if tmp directory exists
        if not os.path.exists("tmp"):
            self.enable_logging = False
            return
        
        log_filename = f"tmp/log_parser_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        try:
            logging.basicConfig(
                filename=log_filename,
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                force=True  # Overwrite any existing configuration
            )
            self.logger = logging.getLogger(__name__)
            # Test if we can write to the log file
            self.logger.info("LogParser initialized")
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

    # ---------- Read raw logs ----------
    def from_file(self, file_path: str):
        """Load logs from a single file."""
        self._log_info(f"Reading logs from file: {file_path}")
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                self.raw_logs = [line.strip() for line in f if line.strip()]
            self._log_info(f"Successfully read {len(self.raw_logs)} lines from {file_path}")
            return self
        except FileNotFoundError:
            self._log_warning(f"File not found: {file_path}")
            return self
        except Exception as e:
            self._log_warning(f"Error reading file {file_path}: {str(e)}")
            return self

    def from_folder(self, folder_path: str):
        """Load logs from all files in a folder."""
        self._log_info(f"Reading logs from folder: {folder_path}")
        logs = []
        try:
            file_count = 0
            total_lines = 0
            for fname in os.listdir(folder_path):
                path = os.path.join(folder_path, fname)
                if os.path.isfile(path):
                    file_count += 1
                    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                        file_lines = [line.strip() for line in f if line.strip()]
                        logs.extend(file_lines)
                        total_lines += len(file_lines)
                        self._log_info(f"  - Read {len(file_lines)} lines from {fname}")
            
            self.raw_logs = logs
            self._log_info(f"Successfully read {total_lines} lines from {file_count} files in {folder_path}")
            return self
        except FileNotFoundError:
            self._log_warning(f"Folder not found: {folder_path}")
            return self
        except Exception as e:
            self._log_warning(f"Error reading folder {folder_path}: {str(e)}")
            return self

    def from_udp_port(self, host='0.0.0.0', port=514, max_logs=1000):
        """Listen on UDP port for logs."""
        self._log_info(f"Listening for logs on UDP port {port} (host: {host}, max_logs: {max_logs})")
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.bind((host, port))
            self._log_info(f"Successfully bound to {host}:{port}")
        except Exception as e:
            self._log_warning(f"Failed to bind to {host}:{port}: {str(e)}")
            sock.close()
            return self
            
        logs = []
        count = 0
        try:
            while count < max_logs:
                data, addr = sock.recvfrom(4096)
                logs.append(data.decode('utf-8', errors='ignore').strip())
                count += 1
                if count % 100 == 0:  # Log progress every 100 logs
                    self._log_info(f"Received {count} logs so far...")
        except Exception as e:
            self._log_warning(f"Error receiving data: {str(e)}")
        finally:
            sock.close()
            
        self.raw_logs = logs
        self._log_info(f"Finished listening. Received {len(logs)} logs")
        return self

    # ---------- Normalize logs ----------
    def normalize(self) -> pd.DataFrame:
        """
        Returns a Pandas DataFrame with normalized columns:
        timestamp, host, process, pid, message, src_ip, user, event_type (optional)
        """
        self._log_info(f"Normalizing {len(self.raw_logs)} raw log entries")
        
        parsed = []
        successfully_parsed = 0
        failed_parsing = 0
        
        for i, line in enumerate(self.raw_logs):
            # Attempt flexible parsing with regex
            m = re.match(r'^(\w+\s+\d+\s+\d+:\d+:\d+)\s+(\S+)\s+(\S+)\[(\d+)\]:\s+(.*)$', line)
            if m:
                ts_str, host, process, pid, msg = m.groups()
                try:
                    ts = date_parser.parse(ts_str)
                except Exception:
                    ts = None
                
                # Optionally extract IP or user from message
                src_ip_match = re.search(r'(\d{1,3}(?:\.\d{1,3}){3})', msg)
                src_ip = src_ip_match.group(1) if src_ip_match else None
                
                user_match = re.search(r'user (\S+)', msg)
                user = user_match.group(1) if user_match else None
                
                parsed.append({
                    'timestamp': ts,
                    'host': host,
                    'process': process,
                    'pid': int(pid) if pid.isdigit() else None,
                    'message': msg,
                    'src_ip': src_ip,
                    'user': user
                })
                successfully_parsed += 1
            else:
                # Handle lines that don't match the expected format
                parsed.append({
                    'timestamp': None,
                    'host': None,
                    'process': None,
                    'pid': None,
                    'message': line,
                    'src_ip': None,
                    'user': None
                })
                failed_parsing += 1
        
        df = pd.DataFrame(parsed)
        self._log_info(f"Normalization complete. Successfully parsed: {successfully_parsed}, Failed parsing: {failed_parsing}")
        self._log_info(f"Resulting DataFrame shape: {df.shape}")
        
        # Log some statistics
        valid_timestamps = df['timestamp'].notna().sum()
        self._log_info(f"Entries with valid timestamps: {valid_timestamps}/{len(df)} ({100*valid_timestamps/len(df):.1f}%)")
        
        return df
