# Portfolio Monitor Pro

## Directory Structure

```
├── dashboard.html          # Main dashboard interface
├── index.html             # Landing page
├── styles.css             # Styling
├── assets/                # Images and static assets
├── collect/               # Data collection directory
├── data/                  # CSV data files
│   └── lpxd_external_advisors_DF_20250915-2355.csv
└── scripts/               # JavaScript and Python scripts
    ├── csv-monitor.js
    ├── csv-monitor-server.py
    ├── portfolio-logger.js
    └── start-monitoring.sh
```

## Quick Start

1. **Start the web server:**
   ```bash
   python3 -m http.server 8000
   ```

2. **Start CSV monitoring (optional):**
   ```bash
   ./scripts/start-monitoring.sh
   ```

3. **Access the dashboard:**
   - Landing page: http://localhost:8000/index.html
   - Dashboard: http://localhost:8000/dashboard.html

## Key Files

- **dashboard.html**: Main monitoring interface with portfolio positions and P&L
- **data/lpxd_external_advisors_DF_20250915-2355.csv**: Portfolio positions data
- **scripts/csv-monitor.js**: Client-side CSV file monitoring
- **scripts/csv-monitor-server.py**: Server-side CSV monitoring API
