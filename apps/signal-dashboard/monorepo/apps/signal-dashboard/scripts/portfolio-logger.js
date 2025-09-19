/**
 * Portfolio Data Logger
 * Logs portfolio metrics to CSV before and after each rebalancing
 */

class PortfolioLogger {
    constructor() {
        this.logFileName = 'portfolio_daily_log.csv'; // Real log file (not sample)
        this.logHeaders = [
            'timestamp',
            'date',
            'time_utc',
            'time_paris',
            'csv_filename',
            'action', // 'pre_execution' or 'post_execution'
            'portfolio_value',
            'daily_pnl',
            'daily_pnl_percent',
            'cumulative_pnl',
            'total_positions',
            'long_positions',
            'short_positions',
            'long_notional',
            'short_notional',
            'total_notional_at_entry',
            'gross_exposure',
            'net_exposure',
            'top_long_symbol',
            'top_long_weight',
            'top_short_symbol',
            'top_short_weight',
            'hit_rate_estimate',
            'avg_win',
            'avg_loss',
            'reliability_ratio'
        ];
        
        this.initializeLogFile();
    }
    
    /**
     * Initialize the log file with headers if it doesn't exist
     */
    initializeLogFile() {
        try {
            // Check if file exists by trying to read it
            fetch(this.logFileName)
                .then(response => {
                    if (!response.ok) {
                        // File doesn't exist, create it with headers
                        this.createLogFile();
                    }
                })
                .catch(() => {
                    // File doesn't exist, create it with headers
                    this.createLogFile();
                });
        } catch (error) {
            console.log('📝 Initializing portfolio log file...');
            this.createLogFile();
        }
    }
    
    /**
     * Create the log file with headers (NO DOWNLOAD - just log)
     */
    createLogFile() {
        const headersRow = this.logHeaders.join(',') + '\n';
        console.log('📝 Portfolio log file headers (ready for server-side CSV):');
        console.log('Headers:', headersRow);
        console.log('File should be created server-side:', this.logFileName);
    }
    
    /**
     * Log portfolio data before CSV execution
     */
    async logPreExecution(csvFilename) {
        console.log('📊 Logging pre-execution portfolio data...');
        
        const logData = await this.gatherPortfolioData(csvFilename, 'pre_execution');
        await this.appendToLog(logData);
        
        console.log('✅ Pre-execution data logged');
    }
    
    /**
     * Log portfolio data after CSV execution
     */
    async logPostExecution(csvFilename) {
        console.log('📊 Logging post-execution portfolio data...');
        
        const logData = await this.gatherPortfolioData(csvFilename, 'post_execution');
        await this.appendToLog(logData);
        
        // Add data point to portfolio value chart
        if (window.addPortfolioValueDataPoint && logData.portfolio_value) {
            window.addPortfolioValueDataPoint(logData.date, logData.portfolio_value);
        }
        
        console.log('✅ Post-execution data logged');
    }
    
    /**
     * Gather all portfolio data for logging
     */
    async gatherPortfolioData(csvFilename, action) {
        const now = new Date();
        const timestamp = now.toISOString();
        const date = now.toISOString().split('T')[0];
        const timeUTC = now.toISOString().substr(11, 8);
        const timeParis = now.toLocaleString('en-GB', {
            timeZone: 'Europe/Paris',
            hour12: false,
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
        
        // Get portfolio metrics
        const portfolioValue = this.getPortfolioValue();
        const dailyPnL = this.getDailyPnL();
        const dailyPnLPercent = this.getDailyPnLPercent();
        const cumulativePnL = this.getCumulativePnL();
        const positionCounts = this.getPositionCounts();
        const exposureData = this.getExposureData();
        const topPositions = this.getTopPositions();
        const performanceMetrics = this.getPerformanceMetrics();
        const totalNotionalData = this.getTotalNotionalData();
        
        return {
            timestamp,
            date,
            time_utc: timeUTC,
            time_paris: timeParis,
            csv_filename: csvFilename,
            action,
            portfolio_value: portfolioValue,
            daily_pnl: dailyPnL,
            daily_pnl_percent: dailyPnLPercent,
            cumulative_pnl: cumulativePnL,
            total_positions: positionCounts.total,
            long_positions: positionCounts.long,
            short_positions: positionCounts.short,
            long_notional: positionCounts.longNotional,
            short_notional: positionCounts.shortNotional,
            total_notional_at_entry: totalNotionalData.long + totalNotionalData.short,
            gross_exposure: exposureData.gross,
            net_exposure: exposureData.net,
            top_long_symbol: topPositions.long.symbol,
            top_long_weight: topPositions.long.weight,
            top_short_symbol: topPositions.short.symbol,
            top_short_weight: topPositions.short.weight,
            hit_rate_estimate: performanceMetrics.hitRate,
            avg_win: performanceMetrics.avgWin,
            avg_loss: performanceMetrics.avgLoss,
            reliability_ratio: performanceMetrics.reliability
        };
    }
    
    /**
     * Get current portfolio value
     */
    getPortfolioValue() {
        const element = document.getElementById('portfolio-value');
        if (element) {
            const text = element.textContent.replace(/[$,]/g, '');
            return parseFloat(text) || 0;
        }
        return 0;
    }
    
    /**
     * Get daily P&L
     */
    getDailyPnL() {
        const element = document.getElementById('daily-pnl');
        if (element) {
            const text = element.textContent.replace(/[$,]/g, '');
            return parseFloat(text) || 0;
        }
        return 0;
    }
    
    /**
     * Get daily P&L percentage
     */
    getDailyPnLPercent() {
        const element = document.getElementById('pnl-percent');
        if (element) {
            const text = element.textContent.replace(/[%,]/g, '');
            return parseFloat(text) || 0;
        }
        return 0;
    }
    
    /**
     * Get cumulative P&L since inception
     */
    getCumulativePnL() {
        // This will be set by the dashboard when it loads historical data
        return window.cumulativePnL || 0;
    }
    
    /**
     * Get total notional amounts for long and short positions
     */
    getTotalNotionalData() {
        const longElement = document.getElementById('total-long-notional');
        const shortElement = document.getElementById('total-short-notional');
        
        const longNotional = longElement ? parseFloat(longElement.textContent.replace(/[$,]/g, '')) || 0 : 0;
        const shortNotional = shortElement ? parseFloat(shortElement.textContent.replace(/[$,]/g, '')) || 0 : 0;
        
        return {
            long: longNotional,
            short: shortNotional
        };
    }
    
    /**
     * Get position counts and notional amounts
     */
    getPositionCounts() {
        if (!portfolioData || portfolioData.length === 0) {
            return { total: 0, long: 0, short: 0, longNotional: 0, shortNotional: 0 };
        }
        
        let longCount = 0;
        let shortCount = 0;
        let longNotional = 0;
        let shortNotional = 0;
        
        portfolioData.forEach(position => {
            const notional = parseFloat(position.target_notional);
            const side = parseFloat(position.target_contracts) > 0 ? 'LONG' : 'SHORT';
            
            if (side === 'LONG') {
                longCount++;
                longNotional += notional;
            } else {
                shortCount++;
                shortNotional += Math.abs(notional);
            }
        });
        
        return {
            total: longCount + shortCount,
            long: longCount,
            short: shortCount,
            longNotional,
            shortNotional
        };
    }
    
    /**
     * Get exposure data
     */
    getExposureData() {
        if (!portfolioData || portfolioData.length === 0) {
            return { gross: 0, net: 0 };
        }
        
        let grossExposure = 0;
        let netExposure = 0;
        
        portfolioData.forEach(position => {
            const notional = parseFloat(position.target_notional);
            grossExposure += Math.abs(notional);
            netExposure += notional;
        });
        
        return {
            gross: grossExposure,
            net: netExposure
        };
    }
    
    /**
     * Get top positions by weight
     */
    getTopPositions() {
        if (!portfolioData || portfolioData.length === 0) {
            return { long: { symbol: 'N/A', weight: 0 }, short: { symbol: 'N/A', weight: 0 } };
        }
        
        let topLong = { symbol: 'N/A', weight: 0 };
        let topShort = { symbol: 'N/A', weight: 0 };
        
        portfolioData.forEach(position => {
            const symbol = position.ticker || position.ric || position.internal_code;
            const notional = parseFloat(position.target_notional);
            const weight = (notional / 1000000) * 100;
            const side = parseFloat(position.target_contracts) > 0 ? 'LONG' : 'SHORT';
            
            if (side === 'LONG' && Math.abs(weight) > Math.abs(topLong.weight)) {
                topLong = { symbol, weight };
            } else if (side === 'SHORT' && Math.abs(weight) > Math.abs(topShort.weight)) {
                topShort = { symbol, weight };
            }
        });
        
        return { long: topLong, short: topShort };
    }
    
    /**
     * Get performance metrics (estimated)
     */
    getPerformanceMetrics() {
        // These are estimated metrics - in a real system, you'd calculate from historical data
        const dailyPnL = this.getDailyPnL();
        const portfolioValue = this.getPortfolioValue();
        
        // Simple estimation based on current P&L
        const hitRate = dailyPnL > 0 ? 0.6 : 0.4; // Estimate based on current performance
        const avgWin = dailyPnL > 0 ? Math.abs(dailyPnL) * 1.2 : Math.abs(dailyPnL) * 0.8;
        const avgLoss = dailyPnL < 0 ? Math.abs(dailyPnL) * 1.2 : Math.abs(dailyPnL) * 0.8;
        const reliability = avgLoss > 0 ? avgWin / avgLoss : 1;
        
        return {
            hitRate: (hitRate * 100).toFixed(2),
            avgWin: avgWin.toFixed(2),
            avgLoss: avgLoss.toFixed(2),
            reliability: reliability.toFixed(2)
        };
    }
    
    /**
     * Append data to the log file (NO DOWNLOAD - just log to console)
     */
    async appendToLog(logData) {
        const row = this.logHeaders.map(header => {
            const value = logData[header] || '';
            // Escape commas and quotes in CSV
            if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {
                return `"${value.replace(/"/g, '""')}"`;
            }
            return value;
        }).join(',') + '\n';
        
        // Just log the data - NO DOWNLOAD
        console.log('📝 Portfolio data logged (ready for server-side CSV file):');
        console.log('CSV Row:', row);
        console.log('Portfolio Data:', logData);
        
        // In a real implementation, this would append to a server-side CSV file
        // For now, just log the data that should be written to the CSV
    }
    
    /**
     * REMOVED: Download function - no downloads wanted
     * Portfolio data should be logged to server-side CSV file only
     */
    
    /**
     * Get log file content (for display)
     */
    async getLogContent() {
        try {
            const response = await fetch(this.logFileName);
            if (response.ok) {
                return await response.text();
            }
        } catch (error) {
            console.log('📝 Log file not found or not accessible');
        }
        return null;
    }
    
    /**
     * Get recent log entries
     */
    async getRecentLogs(days = 7) {
        const content = await this.getLogContent();
        if (!content) return [];
        
        const lines = content.trim().split('\n');
        const headers = lines[0].split(',');
        const data = [];
        
        for (let i = 1; i < lines.length; i++) {
            const values = lines[i].split(',');
            const entry = {};
            headers.forEach((header, index) => {
                entry[header] = values[index] || '';
            });
            data.push(entry);
        }
        
        // Filter recent entries
        const cutoffDate = new Date();
        cutoffDate.setDate(cutoffDate.getDate() - days);
        
        return data.filter(entry => {
            const entryDate = new Date(entry.date);
            return entryDate >= cutoffDate;
        });
    }
}

// Export for use in other files
window.PortfolioLogger = PortfolioLogger;
