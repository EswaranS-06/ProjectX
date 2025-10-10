import pandas as pd
import re
from dateutil import parser as date_parser
import socket
import os
from typing import List, Dict, Optional
import logging
from datetime import datetime, timezone
from pathlib import Path
import json
import ipaddress

class LogParser:
    """
    Enhanced log parser with support for multiple log formats and integration 
    with the existing Drain3-based pipeline.
    """

    def __init__(self, enable_logging=True):
        self.raw_logs = []
        self.enable_logging = enable_logging
        self.logger = None
        if self.enable_logging:
            self._setup_logging()

    def _setup_logging(self):
        """Set up logging configuration."""
        # Create tmp directory if it doesn't exist
        if not os.path.exists("tmp"):
            try:
                os.makedirs("tmp")
            except Exception as e:
                print(f"Warning: Could not create tmp directory: {e}")
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
            self.logger.info("Enhanced LogParser initialized")
        except Exception as e:
            print(f"Warning: Could not set up logging to {log_filename}: {e}")
            self.enable_logging = False

    def _log_info(self, message):
        """Log info message if logging is enabled."""
        if self.enable_logging and self.logger:
            try:
                self.logger.info(message)
            except Exception:
                pass  # Silently fail if logging fails
        print(message)  # Also print to console

    def _log_warning(self, message):
        """Log warning message if logging is enabled."""
        if self.enable_logging and self.logger:
            try:
                self.logger.warning(message)
            except Exception:
                pass  # Silently fail if logging fails
        print(f"WARNING: {message}")  # Also print to console

    def _log_error(self, message):
        """Log error message if logging is enabled."""
        if self.enable_logging and self.logger:
            try:
                self.logger.error(message)
            except Exception:
                pass  # Silently fail if logging fails
        print(f"ERROR: {message}")  # Also print to console

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
                    try:
                        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                            file_lines = [line.strip() for line in f if line.strip()]
                            logs.extend(file_lines)
                            total_lines += len(file_lines)
                            if len(file_lines) > 0:
                                self._log_info(f"  - Read {len(file_lines)} lines from {fname}")
                    except Exception as e:
                        self._log_warning(f"Error reading file {fname}: {str(e)}")
                        continue
            
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

    # ---------- Enhanced log parsing ----------
    def normalize(self) -> pd.DataFrame:
        """
        Enhanced normalization that produces columns compatible with the feature engineering pipeline.
        Returns a Pandas DataFrame with normalized columns:
        timestamp, source_file, level, indicator_tags_list, ip_src, ip_dst, 
        service, message, peer_port, line_number, ip_src_valid, ip_dst_valid, 
        message_raw, day_of_week, hour_of_day, is_weekend
        """
        self._log_info(f"Normalizing {len(self.raw_logs)} raw log entries with enhanced parser")
        
        parsed = []
        successfully_parsed = 0
        failed_parsing = 0
        
        # Enhanced regex patterns for different log formats
        patterns = [
            # Apache/Nginx combined log format
            r'(?P<ip>\d{1,3}(?:\.\d{1,3}){3}) - - \[(?P<timestamp>[^\]]+)\] "(?P<method>\w+) (?P<path>[^"]*)" (?P<status>\d+) (?P<size>\d+) "(?P<referer>[^"]*)" "(?P<user_agent>[^"]*)"',
            
            # Syslog format
            r'(?P<timestamp>\w+\s+\d+\s+\d+:\d+:\d+)\s+(?P<host>\S+)\s+(?P<process>\S+)(?:\[(?P<pid>\d+)\])?:\s+(?P<message>.*)',
            
            # Windows Event Log format
            r'TimeGenerated:\s*(?P<timestamp>\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}),\s*EventID:\s*(?P<event_id>\d+),\s*Level:\s*(?P<level>\w+),\s*Source:\s*(?P<source>[^,]+),\s*Message:\s*(?P<message>.*)',
            
            # Generic format with timestamp and level
            r'(?P<timestamp>\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\s*(?:\[(?P<level>\w+)\])?\s*(?P<message>.*)',
            
            # Fallback pattern for any line
            r'(?P<message>.*)'
        ]
        
        # Level mapping
        level_mapping = {
            'emerg': 'CRITICAL', 'emergency': 'CRITICAL', 'fatal': 'CRITICAL',
            'alert': 'CRITICAL', 'crit': 'CRITICAL', 'critical': 'CRITICAL',
            'err': 'ERROR', 'error': 'ERROR',
            'warn': 'WARNING', 'warning': 'WARNING',
            'notice': 'INFO', 'info': 'INFO', 'information': 'INFO',
            'debug': 'DEBUG', 'trace': 'DEBUG'
        }
        
        for i, line in enumerate(self.raw_logs):
            parsed_entry = {
                'timestamp': None,
                'source_file': 'unknown',
                'level': 'INFO',
                'indicator_tags_list': [],
                'ip_src': '',
                'ip_dst': '',
                'service': '',
                'message': line,
                'peer_port': None,
                'line_number': i + 1,
                'ip_src_valid': False,
                'ip_dst_valid': False,
                'message_raw': line,
                'day_of_week': None,
                'hour_of_day': None,
                'is_weekend': False
            }
            
            matched = False
            # Try each pattern
            for pattern in patterns[:-1]:  # Skip the fallback pattern for now
                m = re.match(pattern, line)
                if m:
                    groups = m.groupdict()
                    
                    # Parse timestamp
                    if 'timestamp' in groups and groups['timestamp']:
                        try:
                            ts_str = groups['timestamp']
                            # Handle different timestamp formats
                            if '/' in ts_str and ':' in ts_str:
                                # Apache format: 10/Oct/2000:13:55:36 -0700
                                ts = datetime.strptime(ts_str.split()[0], '%d/%b/%Y:%H:%M:%S')
                            else:
                                ts = date_parser.parse(ts_str)
                            
                            # Ensure timezone awareness
                            if ts.tzinfo is None:
                                ts = ts.replace(tzinfo=timezone.utc)
                            parsed_entry['timestamp'] = ts.isoformat()
                        except Exception:
                            pass
                    
                    # Parse level
                    if 'level' in groups and groups['level']:
                        level = groups['level'].lower()
                        parsed_entry['level'] = level_mapping.get(level, level.upper())
                    
                    # Extract IPs
                    if 'ip' in groups and groups['ip']:
                        ip = groups['ip']
                        if self._is_valid_ip(ip):
                            parsed_entry['ip_src'] = ip
                            parsed_entry['ip_src_valid'] = True
                    
                    # Extract message
                    if 'message' in groups and groups['message']:
                        parsed_entry['message'] = groups['message']
                        parsed_entry['message_raw'] = groups['message']
                        
                        # Extract additional IPs from message
                        ips = re.findall(r'\b(?:\d{1,3}\.){3}\d{1,3}\b', groups['message'])
                        if len(ips) >= 1 and not parsed_entry['ip_src']:
                            if self._is_valid_ip(ips[0]):
                                parsed_entry['ip_src'] = ips[0]
                                parsed_entry['ip_src_valid'] = True
                        if len(ips) >= 2 and not parsed_entry['ip_dst']:
                            if self._is_valid_ip(ips[1]):
                                parsed_entry['ip_dst'] = ips[1]
                                parsed_entry['ip_dst_valid'] = True
                        
                        # Extract port from message
                        port_match = re.search(r'(?:port|Port)[\s:]+(\d+)|[:/](\d+)', groups['message'])
                        if port_match:
                            try:
                                port = port_match.group(1) or port_match.group(2)
                                parsed_entry['peer_port'] = int(port)
                            except (ValueError, IndexError):
                                pass
                    
                    # Extract service/process
                    if 'process' in groups and groups['process']:
                        parsed_entry['service'] = groups['process']
                    elif 'source' in groups and groups['source']:
                        parsed_entry['service'] = groups['source']
                    
                    matched = True
                    successfully_parsed += 1
                    break
            
            # If no pattern matched, use fallback
            if not matched:
                # Try to extract timestamp and level even from unstructured logs
                ts_match = re.search(r'(\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)', line)
                if ts_match:
                    try:
                        ts = date_parser.parse(ts_match.group(1))
                        # Ensure timezone awareness
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=timezone.utc)
                        parsed_entry['timestamp'] = ts.isoformat()
                    except Exception:
                        pass
                
                level_match = re.search(r'\b(ERROR|WARN|WARNING|INFO|DEBUG|CRITICAL|FATAL)\b', line, re.IGNORECASE)
                if level_match:
                    level = level_match.group(1).upper()
                    parsed_entry['level'] = level_mapping.get(level.lower(), level)
                
                # Extract IPs from any part of the line
                ips = re.findall(r'\b(?:\d{1,3}\.){3}\d{1,3}\b', line)
                if ips:
                    if self._is_valid_ip(ips[0]):
                        parsed_entry['ip_src'] = ips[0]
                        parsed_entry['ip_src_valid'] = True
                    if len(ips) > 1 and self._is_valid_ip(ips[1]):
                        parsed_entry['ip_dst'] = ips[1]
                        parsed_entry['ip_dst_valid'] = True
                
                failed_parsing += 1
            
            # Extract indicator tags
            indicators = self._extract_indicators(parsed_entry['message'])
            parsed_entry['indicator_tags_list'] = indicators
            
            # Add temporal features if timestamp is available
            if parsed_entry['timestamp']:
                try:
                    ts = date_parser.parse(parsed_entry['timestamp'])
                    parsed_entry['day_of_week'] = ts.strftime('%A')
                    parsed_entry['hour_of_day'] = ts.hour
                    parsed_entry['is_weekend'] = ts.weekday() >= 5
                except Exception:
                    pass
            
            parsed.append(parsed_entry)
        
        df = pd.DataFrame(parsed)
        self._log_info(f"Enhanced normalization complete. Successfully parsed: {successfully_parsed}, Failed parsing: {failed_parsing}")
        self._log_info(f"Resulting DataFrame shape: {df.shape}")
        
        # Log some statistics
        if 'timestamp' in df.columns:
            valid_timestamps = df['timestamp'].notna().sum()
            self._log_info(f"Entries with valid timestamps: {valid_timestamps}/{len(df)} ({100*valid_timestamps/len(df):.1f}%)")
        
        return df
    
    def _is_valid_ip(self, ip: str) -> bool:
        """Check if IP address is valid."""
        if not ip:
            return False
        try:
            ipaddress.ip_address(ip)
            return True
        except ValueError:
            return False
    
    def _extract_indicators(self, message: str) -> List[str]:
        """Extract indicator tags from message."""
        if not message:
            return []
        
        indicators = []
        message_lower = message.lower()
        
        # Error indicators
        if re.search(r'\b(error|failed|failure|invalid|exception)\b', message_lower):
            indicators.append('error')
        
        # Warning indicators
        if re.search(r'\b(warning|warn|attention)\b', message_lower):
            indicators.append('warning')
        
        # Success indicators
        if re.search(r'\b(success|successful|completed|ok)\b', message_lower):
            indicators.append('success')
        
        # Security indicators
        if re.search(r'\b(security|auth|authentication|permission|login|logout)\b', message_lower):
            indicators.append('security')
        
        # Network indicators
        if re.search(r'\b(connect|disconnect|receive|send|packet)\b', message_lower):
            indicators.append('network')
        
        return indicators

    def save_output(self, df: pd.DataFrame, output_dir: str = "oplogs/csv/", filename: str = "parsed_logs"):
        """Save the parsed DataFrame to CSV and JSON formats."""
        try:
            # Create output directory
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # Save as CSV
            csv_path = Path(output_dir) / f"{filename}.csv"
            df.to_csv(csv_path, index=False)
            self._log_info(f"Saved parsed logs to {csv_path}")
            
            # Save as JSON
            json_path = Path(output_dir) / f"{filename}.json"
            df.to_json(json_path, orient='records', lines=True)
            self._log_info(f"Saved parsed logs to {json_path}")
            
            return True
        except Exception as e:
            self._log_error(f"Error saving output: {str(e)}")
            return False
