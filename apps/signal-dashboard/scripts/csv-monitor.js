// CSV File Monitoring System
class CSVMonitor {
    constructor() {
        this.currentCSV = null;
        this.lastModified = null;
        this.checkInterval = 30000; // Default: Check every 30 seconds
        this.isMonitoring = false;
        this.onRebalanceCallback = null;
        this.onNewCSVCallback = null;
        this.csvReleaseWindow = { start: 0, end: 0.5 }; // 00:00 - 00:30
    }

    // Start monitoring for new CSV files
    startMonitoring() {
        if (this.isMonitoring) return;
        
        console.log('🔄 Starting CSV monitoring...');
        this.isMonitoring = true;
        
        // Initial check
        this.checkForNewCSV();
        
        // Set up interval checking with dynamic timing
        this.monitorInterval = setInterval(() => {
            this.checkForNewCSV();
            this.updateCheckInterval();
        }, this.checkInterval);
    }

    // Stop monitoring
    stopMonitoring() {
        if (!this.isMonitoring) return;
        
        console.log('⏹️ Stopping CSV monitoring...');
        this.isMonitoring = false;
        
        if (this.monitorInterval) {
            clearInterval(this.monitorInterval);
            this.monitorInterval = null;
        }
    }

    // Check for new CSV files
    async checkForNewCSV() {
        try {
            // Get list of CSV files in the directory
            const csvFiles = await this.getCSVFiles();
            
            if (csvFiles.length === 0) {
                console.log('📁 No CSV files found');
                return;
            }

            // Find the latest CSV file
            const latestCSV = this.findLatestCSV(csvFiles);
            
            if (!latestCSV) {
                console.log('❌ No valid CSV files found');
                return;
            }

            // Check if this is a new CSV file
            if (this.currentCSV !== latestCSV.filename || 
                this.lastModified !== latestCSV.modified) {
                
                console.log(`🆕 New CSV detected: ${latestCSV.filename}`);
                console.log(`📅 Modified: ${new Date(latestCSV.modified).toLocaleString()}`);
                
                // Update current CSV tracking
                this.currentCSV = latestCSV.filename;
                this.lastModified = latestCSV.modified;
                
                // Trigger rebalancing
                await this.triggerRebalancing(latestCSV.filename);
            }
            
        } catch (error) {
            console.error('❌ Error checking for new CSV:', error);
        }
    }

    // Get list of CSV files from the monitoring server
    async getCSVFiles() {
        try {
            const response = await fetch('http://localhost:8001/api/csv-files');
            if (response.ok) {
                const data = await response.json();
                return data.files || [];
            }
        } catch (error) {
            console.log('📁 CSV monitoring server not available, falling back to local file');
            // Fallback to local file if server is not running
            try {
                const response = await fetch('lpxd_external_advisors_DF_20250916-0806.csv', { method: 'HEAD' });
                if (response.ok) {
                    return [{
                        filename: 'lpxd_external_advisors_DF_20250916-0806.csv',
                        modified: new Date().toISOString(),
                        size: response.headers.get('content-length') || '0'
                    }];
                }
            } catch (fallbackError) {
                console.log('📁 No CSV files accessible');
            }
        }
        
        return [];
    }

    // Find the preferred CSV file (2355.csv first, then latest)
    findLatestCSV(csvFiles) {
        // ONLY look for 2355.csv files - NO FALLBACK
        const pattern = /lpxd_external_advisors_DF_(\d{8}-\d{4})\.csv/;
        
        const validFiles = csvFiles.filter(file => pattern.test(file.filename));
        
        if (validFiles.length === 0) {
            console.error('❌ NO CSV FILES FOUND AT ALL!');
            return null;
        }
        
        // ONLY accept 2355.csv files
        const preferredFile = validFiles.find(file => file.filename.endsWith('2355.csv'));
        if (preferredFile) {
            console.log('✅ FOUND 2355.csv file:', preferredFile.filename);
            return preferredFile;
        }
        
        // NO FALLBACK - throw error if no 2355.csv
        console.error('❌ NO 2355.csv FILE FOUND! Available files:', validFiles.map(f => f.filename));
        throw new Error('2355.csv file is required but not found!');
    }

    // Trigger portfolio rebalancing
    async triggerRebalancing(csvFilename) {
        try {
            console.log(`🔄 Starting rebalancing with ${csvFilename}...`);
            
            // Update status bar instead of popup
            this.updateCSVStatus(csvFilename);
            
            // Try to load from monitoring server first
            let csvText = null;
            try {
                const response = await fetch(`http://localhost:8001/api/csv-content/${csvFilename}`);
                if (response.ok) {
                    const data = await response.json();
                    csvText = data.content;
                    console.log(`📡 Loaded CSV from monitoring server`);
                }
            } catch (serverError) {
                console.log('📁 Monitoring server not available, trying local file');
            }
            
            // Fallback to local file if server failed
            if (!csvText) {
                const response = await fetch(csvFilename);
                if (!response.ok) {
                    throw new Error(`Failed to load ${csvFilename}`);
                }
                csvText = await response.text();
                console.log(`📁 Loaded CSV from local file`);
            }
            
            const newPortfolioData = this.parseCSV(csvText);
            
            console.log(`📊 Loaded ${newPortfolioData.length} positions from new CSV`);
            
            // Trigger callback to update portfolio
            if (this.onRebalanceCallback) {
                await this.onRebalanceCallback(newPortfolioData, csvFilename);
            }
            
            // Show success notification (brief)
            this.showNotification(`Portfolio rebalanced with ${newPortfolioData.length} positions`, 'success');
            
        } catch (error) {
            console.error('❌ Error during rebalancing:', error);
            this.showNotification(`Rebalancing failed: ${error.message}`, 'error');
        }
    }

    // Parse CSV data
    parseCSV(csvText) {
        const lines = csvText.trim().split('\n');
        const headers = lines[0].split(',');
        const data = [];
        
        for (let i = 1; i < lines.length; i++) {
            const values = lines[i].split(',');
            const row = {};
            headers.forEach((header, index) => {
                row[header.trim()] = values[index] ? values[index].trim() : '';
            });
            data.push(row);
        }
        
        return data;
    }

    // Show notification to user
    showNotification(message, type = 'info') {
        // Create notification element
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.innerHTML = `
            <div class="notification-content">
                <span class="notification-icon">${this.getNotificationIcon(type)}</span>
                <span class="notification-message">${message}</span>
                <button class="notification-close" onclick="this.parentElement.parentElement.remove()">×</button>
            </div>
        `;
        
        // Add to page
        document.body.appendChild(notification);
        
        // Auto-remove after 5 seconds
        setTimeout(() => {
            if (notification.parentElement) {
                notification.remove();
            }
        }, 5000);
    }

    // Get notification icon based on type
    getNotificationIcon(type) {
        const icons = {
            info: 'ℹ️',
            success: '✅',
            error: '❌',
            warning: '⚠️'
        };
        return icons[type] || icons.info;
    }

    // Set callback for rebalancing
    setOnRebalance(callback) {
        this.onRebalanceCallback = callback;
    }

    // Set callback for new CSV detection
    setOnNewCSV(callback) {
        this.onNewCSVCallback = callback;
    }

    // Get current CSV info
    getCurrentCSV() {
        return {
            filename: this.currentCSV,
            lastModified: this.lastModified
        };
    }
    
    // Update check interval based on time of day
    updateCheckInterval() {
        const now = new Date();
        const currentHour = now.getHours();
        const currentMinute = now.getMinutes();
        const currentTime = currentHour + currentMinute / 60.0;
        
        // Check if we're in the CSV release window (00:00 - 00:30)
        const inReleaseWindow = (currentTime >= this.csvReleaseWindow.start && 
                               currentTime <= this.csvReleaseWindow.end);
        
        let newInterval;
        if (inReleaseWindow) {
            newInterval = 10000; // Check every 10 seconds during release window
            console.log('🕛 In CSV release window (00:00-00:30) - checking every 10s');
        } else {
            newInterval = 60000; // Check every minute outside release window
        }
        
        // Update interval if it changed
        if (newInterval !== this.checkInterval) {
            this.checkInterval = newInterval;
            
            // Restart monitoring with new interval
            if (this.isMonitoring) {
                clearInterval(this.monitorInterval);
                this.monitorInterval = setInterval(() => {
                    this.checkForNewCSV();
                    this.updateCheckInterval();
                }, this.checkInterval);
            }
        }
    }
    
    // Update CSV status bar
    updateCSVStatus(filename) {
        const filenameElement = document.getElementById('current-csv-filename');
        const timestampElement = document.getElementById('csv-timestamp');
        
        if (filenameElement) {
            filenameElement.textContent = filename;
        }
        
        if (timestampElement) {
            const now = new Date();
            const timestamp = now.toLocaleString('en-GB', {
                timeZone: 'Europe/Paris',
                year: 'numeric',
                month: '2-digit',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            });
            timestampElement.textContent = `(Loaded: ${timestamp})`;
        }
    }
}

// Export for use in other files
window.CSVMonitor = CSVMonitor;
