#!/usr/bin/env python3
"""
CSV File Monitoring Server
A lightweight server that monitors CSV files and provides API endpoints
for the monitoring platform to detect new files automatically.
"""

import os
import json
import time
import threading
from datetime import datetime
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import re

class CSVMonitorServer:
    def __init__(self, csv_directory, port=8001):
        self.csv_directory = Path(csv_directory)
        self.port = port
        self.csv_files = {}
        self.last_check = None
        self.running = False
        self.csv_release_window = (0, 0.5)  # CSV released between 00:00 and 00:30
        
        # Ensure directory exists
        self.csv_directory.mkdir(parents=True, exist_ok=True)
        
        print(f"📁 Monitoring directory: {self.csv_directory}")
        print(f"🌐 Server will run on port: {self.port}")
    
    def start_monitoring(self):
        """Start the file monitoring in a separate thread"""
        self.running = True
        monitor_thread = threading.Thread(target=self._monitor_files, daemon=True)
        monitor_thread.start()
        print("🔄 File monitoring started")
    
    def _monitor_files(self):
        """Monitor CSV files in the directory with optimized timing"""
        while self.running:
            try:
                current_hour = datetime.now().hour
                current_minute = datetime.now().minute
                current_time = current_hour + current_minute / 60.0
                
                # Check if we're in the CSV release window (00:00 - 00:30)
                in_release_window = (current_time >= self.csv_release_window[0] and 
                                   current_time <= self.csv_release_window[1])
                
                self._scan_directory()
                
                # Optimize check frequency based on time
                if in_release_window:
                    time.sleep(10)  # Check every 10 seconds during release window
                    print(f"🕛 In CSV release window (00:00-00:30) - checking every 10s")
                else:
                    time.sleep(60)  # Check every minute outside release window
                    
            except Exception as e:
                print(f"❌ Error monitoring files: {e}")
                time.sleep(30)  # Wait longer on error
    
    def _scan_directory(self):
        """Scan directory for CSV files"""
        if not self.csv_directory.exists():
            return
        
        current_files = {}
        
        # Find all CSV files matching the pattern
        pattern = re.compile(r'lpxd_external_advisors_DF_(\d{8}-\d{4})\.csv')
        
        for file_path in self.csv_directory.glob('*.csv'):
            if pattern.match(file_path.name):
                stat = file_path.stat()
                current_files[file_path.name] = {
                    'filename': file_path.name,
                    'path': str(file_path),
                    'size': stat.st_size,
                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    'created': datetime.fromtimestamp(stat.st_ctime).isoformat()
                }
        
        # Check for new files
        for filename, file_info in current_files.items():
            if filename not in self.csv_files:
                print(f"🆕 New CSV detected: {filename}")
                print(f"📅 Modified: {file_info['modified']}")
                print(f"📊 Size: {file_info['size']} bytes")
        
        self.csv_files = current_files
        self.last_check = datetime.now().isoformat()
    
    def get_latest_csv(self):
        """ONLY get 2355.csv files - NO FALLBACK"""
        if not self.csv_files:
            print("❌ NO CSV FILES FOUND!")
            return None
        
        # ONLY accept 2355.csv files
        for filename, file_info in self.csv_files.items():
            if filename.endswith('2355.csv'):
                print(f"✅ FOUND 2355.csv file: {filename}")
                return file_info
        
        # NO FALLBACK - throw error if no 2355.csv
        available_files = list(self.csv_files.keys())
        print(f"❌ NO 2355.csv FILE FOUND! Available files: {available_files}")
        raise Exception("2355.csv file is required but not found!")
    
    def get_all_csvs(self):
        """Get all CSV files"""
        return list(self.csv_files.values())
    
    def get_csv_content(self, filename):
        """Get content of a specific CSV file"""
        if filename not in self.csv_files:
            return None
        
        file_path = self.csv_directory / filename
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            print(f"❌ Error reading {filename}: {e}")
            return None
    
    def log_csv_usage(self, log_data):
        """Log CSV usage to portfolio daily log"""
        try:
            log_file = 'portfolio_daily_log.csv'
            
            # Check if file exists, if not create with headers
            if not os.path.exists(log_file):
                headers = [
                    'timestamp', 'date', 'time_utc', 'time_paris', 'csv_filename', 'action',
                    'portfolio_value', 'daily_pnl', 'daily_pnl_percent', 'cumulative_pnl',
                    'total_positions', 'long_positions', 'short_positions',
                    'long_notional', 'short_notional', 'total_notional_at_entry',
                    'gross_exposure', 'net_exposure',
                    'top_long_symbol', 'top_long_weight', 'top_short_symbol', 'top_short_weight',
                    'hit_rate_estimate', 'avg_win', 'avg_loss', 'reliability_ratio'
                ]
                with open(log_file, 'w', encoding='utf-8') as f:
                    f.write(','.join(headers) + '\n')
            
            # Append log entry
            with open(log_file, 'a', encoding='utf-8') as f:
                row = [
                    log_data.get('timestamp', ''),
                    log_data.get('date', ''),
                    log_data.get('time_utc', ''),
                    log_data.get('time_paris', ''),
                    log_data.get('csv_filename', ''),
                    log_data.get('action', ''),
                    log_data.get('portfolio_value', 0),
                    log_data.get('daily_pnl', 0),
                    log_data.get('daily_pnl_percent', 0),
                    log_data.get('cumulative_pnl', 0),
                    log_data.get('total_positions', 0),
                    log_data.get('long_positions', 0),
                    log_data.get('short_positions', 0),
                    log_data.get('long_notional', 0),
                    log_data.get('short_notional', 0),
                    log_data.get('total_notional_at_entry', 0),
                    log_data.get('gross_exposure', 0),
                    log_data.get('net_exposure', 0),
                    log_data.get('top_long_symbol', ''),
                    log_data.get('top_long_weight', 0),
                    log_data.get('top_short_symbol', ''),
                    log_data.get('top_short_weight', 0),
                    log_data.get('hit_rate_estimate', 0),
                    log_data.get('avg_win', 0),
                    log_data.get('avg_loss', 0),
                    log_data.get('reliability_ratio', 0)
                ]
                f.write(','.join(map(str, row)) + '\n')
            
            print(f"📝 Logged CSV usage: {log_data.get('csv_filename', 'unknown')} at {log_data.get('time_paris', 'unknown')}")
            
        except Exception as e:
            print(f"❌ Error logging CSV usage: {e}")

class CSVMonitorHandler(BaseHTTPRequestHandler):
    def __init__(self, monitor, *args, **kwargs):
        self.monitor = monitor
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests"""
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        
        try:
            if path == '/api/csv-files':
                self._handle_csv_files()
            elif path == '/api/latest-csv':
                self._handle_latest_csv()
            elif path.startswith('/api/csv-content/'):
                filename = path.split('/')[-1]
                self._handle_csv_content(filename)
            elif path == '/api/status':
                self._handle_status()
            else:
                self._send_response(404, {'error': 'Not found'})
        
        except Exception as e:
            self._send_response(500, {'error': str(e)})
    
    def do_POST(self):
        """Handle POST requests"""
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        
        try:
            if path == '/api/log-csv-usage':
                self._handle_log_csv_usage()
            else:
                self._send_response(404, {'error': 'Endpoint not found'})
        
        except Exception as e:
            self._send_response(500, {'error': str(e)})
    
    def _handle_csv_files(self):
        """Return list of all CSV files"""
        csv_files = self.monitor.get_all_csvs()
        self._send_response(200, {
            'files': csv_files,
            'count': len(csv_files),
            'last_check': self.monitor.last_check
        })
    
    def _handle_latest_csv(self):
        """Return the latest CSV file info"""
        latest = self.monitor.get_latest_csv()
        if latest:
            self._send_response(200, latest)
        else:
            self._send_response(404, {'error': 'No CSV files found'})
    
    def _handle_csv_content(self, filename):
        """Return content of a specific CSV file"""
        content = self.monitor.get_csv_content(filename)
        if content:
            self._send_response(200, {
                'filename': filename,
                'content': content,
                'size': len(content)
            })
        else:
            self._send_response(404, {'error': f'File {filename} not found'})
    
    def _handle_status(self):
        """Return server status"""
        self._send_response(200, {
            'status': 'running',
            'monitored_directory': str(self.monitor.csv_directory),
            'csv_count': len(self.monitor.csv_files),
            'last_check': self.monitor.last_check,
            'uptime': time.time() - self.monitor.start_time if hasattr(self.monitor, 'start_time') else 0
        })
    
    def _handle_log_csv_usage(self):
        """Log CSV usage to the portfolio daily log"""
        try:
            # Read the request body
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            log_data = json.loads(post_data.decode('utf-8'))
            
            # Append to portfolio daily log
            self.monitor.log_csv_usage(log_data)
            
            self._send_response(200, {'status': 'logged'})
            
        except Exception as e:
            print(f"❌ Error logging CSV usage: {e}")
            self._send_response(500, {'error': str(e)})
    
    def _send_response(self, status_code, data):
        """Send JSON response"""
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')  # Enable CORS
        self.end_headers()
        
        response = json.dumps(data, indent=2)
        self.wfile.write(response.encode('utf-8'))
    
    def log_message(self, format, *args):
        """Override to reduce log noise"""
        pass

def create_handler(monitor):
    """Create handler with monitor instance"""
    def handler(*args, **kwargs):
        return CSVMonitorHandler(monitor, *args, **kwargs)
    return handler

def main():
    """Main function to start the server"""
    # Configuration - Use local directory for now
    CSV_DIRECTORY = "."  # Current directory where the CSV files are
    PORT = 8001
    
    print("🚀 Starting CSV Monitor Server...")
    print("=" * 50)
    
    # Create monitor instance
    monitor = CSVMonitorServer(CSV_DIRECTORY, PORT)
    monitor.start_time = time.time()
    
    # Start file monitoring
    monitor.start_monitoring()
    
    # Create HTTP server
    handler = create_handler(monitor)
    httpd = HTTPServer(('localhost', PORT), handler)
    
    print(f"✅ Server started successfully!")
    print(f"📡 API Endpoints:")
    print(f"   GET http://localhost:{PORT}/api/csv-files")
    print(f"   GET http://localhost:{PORT}/api/latest-csv")
    print(f"   GET http://localhost:{PORT}/api/csv-content/<filename>")
    print(f"   GET http://localhost:{PORT}/api/status")
    print("=" * 50)
    print("Press Ctrl+C to stop the server")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
        monitor.running = False
        httpd.shutdown()

if __name__ == "__main__":
    main()
