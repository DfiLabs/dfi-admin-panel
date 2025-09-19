'use client'

import { useState } from 'react'
import { motion } from 'framer-motion'
import { Upload, Database, Download, FileText } from 'lucide-react'
import FactsheetDisplay from '../../components/FactsheetDisplay'

export default function FactsheetPage() {
  const [selectedOption, setSelectedOption] = useState<'raw' | 'live' | null>(null)
  const [uploadedFile, setUploadedFile] = useState<File | null>(null)
  const [selectedStrategy, setSelectedStrategy] = useState<'smart-arbitrage' | 'descartes' | 'vision' | null>(null)
  const [showFactsheet, setShowFactsheet] = useState(false)
  const [factsheetData, setFactsheetData] = useState<any>(null)

  const generateTemplate = () => {
    // Generate sample data for the template
    const dates = []
    const performances = []
    const startDate = new Date('2023-01-01')
    
    for (let i = 0; i < 365; i++) {
      const date = new Date(startDate)
      date.setDate(date.getDate() + i)
      dates.push(date.toISOString().split('T')[0])
      performances.push((Math.random() - 0.5) * 10) // Random performance between -5% and 5%
    }

    const csvContent = [
      ['Date', 'Performance'],
      ...dates.map((date, index) => [date, performances[index].toFixed(2)])
    ].map(row => row.join(',')).join('\n')

    const blob = new Blob([csvContent], { type: 'text/csv' })
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = 'factsheet-template.csv'
    a.click()
    window.URL.revokeObjectURL(url)
  }

  const handleFileUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (file && file.type === 'text/csv') {
      setUploadedFile(file)
    } else if (file) {
      alert('Please upload a CSV file only.')
    }
  }

  const handleDrop = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault()
    const file = event.dataTransfer.files[0]
    if (file && file.type === 'text/csv') {
      setUploadedFile(file)
    } else if (file) {
      alert('Please upload a CSV file only.')
    }
  }

  const handleDragOver = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault()
  }

  const calculateMaxDrawdown = (equityCurve: number[]) => {
    let peak = equityCurve[0] // Start with first value
    let maxDrawdown = 0
    
    for (let i = 0; i < equityCurve.length; i++) {
      const currentValue = equityCurve[i]
      
      // Update peak if current value is higher
      if (currentValue > peak) {
        peak = currentValue
      }
      
      // Calculate drawdown: (G_t / H_t) - 1
      const drawdown = (currentValue / peak) - 1
      
      // Update max drawdown if this is worse
      if (drawdown < maxDrawdown) {
        maxDrawdown = drawdown
      }
    }
    
    return maxDrawdown // This will be negative (or 0)
  }

  const generateFactsheet = () => {
    if (uploadedFile && selectedStrategy) {
      // Process the actual CSV data to calculate real metrics
      const reader = new FileReader()
      reader.onload = (e) => {
        const csv = e.target?.result as string
        const lines = csv.split('\n').filter(line => line.trim())
        const headers = lines[0].split(',').map(h => h.trim())
        
        // Find date and performance columns
        const dateIndex = headers.findIndex(h => h.toLowerCase().includes('date'))
        const performanceIndex = headers.findIndex(h => h.toLowerCase().includes('performance') || h.toLowerCase().includes('return'))
        
        if (dateIndex === -1 || performanceIndex === -1) {
          alert('CSV must contain "date" and "performance" columns')
          return
        }
        
        // Parse data rows
        const dataPoints = []
        for (let i = 1; i < lines.length; i++) {
          const values = lines[i].split(',').map(v => v.trim())
          if (values[dateIndex] && values[performanceIndex]) {
            const date = new Date(values[dateIndex])
            const performance = parseFloat(values[performanceIndex])
            if (!isNaN(performance)) {
              dataPoints.push({ date, performance })
            }
          }
        }
        
        if (dataPoints.length === 0) {
          alert('No valid data found in CSV')
          return
        }
        
        // Calculate metrics from actual data using proper formulas
        const returns = dataPoints.map(d => d.performance / 100) // Convert percentages to decimals
        const N = returns.length
        const D_ann = 252 // Trading days convention
        
        // Daily mean (arithmetic mean)
        const mu = returns.reduce((sum, r) => sum + r, 0) / N
        
        // Daily standard deviation (sample st.dev. with N-1)
        const variance = returns.reduce((sum, r) => sum + Math.pow(r - mu, 2), 0) / (N - 1)
        const sigma = Math.sqrt(variance)
        
        // Equity curve (compounded returns)
        const equityCurve = [1] // G_0 = 1
        for (let i = 0; i < N; i++) {
          equityCurve.push(equityCurve[i] * (1 + returns[i]))
        }
        
        // Total Return (geometric total over the sample)
        const totalReturn = equityCurve[N] - 1
        
        // Annualized Return (CAGR)
        const cagr = Math.pow(1 + totalReturn, D_ann / N) - 1
        
        // Volatility (annualized standard deviation)
        const volatility = sigma * Math.sqrt(D_ann)
        
        // Sharpe Ratio (annualized, assuming R_f = 0)
        const sharpeRatio = volatility > 0 ? (mu / sigma) * Math.sqrt(D_ann) : 0
        
        // Max Drawdown (on compounded equity curve)
        const maxDrawdown = calculateMaxDrawdown(equityCurve.slice(1)) // Skip initial 1
        
        // Win Rate (percent of periods with positive return)
        const winRate = (returns.filter(r => r > 0).length / N) * 100
        
        // Total Trades (number of observations)
        const totalTrades = N
        
        // Average Daily Return
        const avgTradeReturn = mu

        const factsheetData = {
          strategy: selectedStrategy,
          fileName: uploadedFile.name.replace('.csv', ''),
          performance: totalReturn * 100, // Convert back to percentage for display
          sharpeRatio: sharpeRatio,
          maxDrawdown: Math.abs(maxDrawdown) * 100, // Convert to positive percentage
          totalReturn: totalReturn * 100,
          volatility: volatility * 100, // Convert to percentage
          winRate: winRate,
          totalTrades: totalTrades,
          avgTradeReturn: avgTradeReturn * 100, // Convert to percentage
          rawData: dataPoints // Store raw data for monthly calculations
        }

        // Create a new factsheet entry for history
        const newFactsheet = {
          id: Date.now().toString(),
          name: `${uploadedFile.name.replace('.csv', '')} - ${strategies[selectedStrategy].name}`,
          strategy: selectedStrategy,
          createdAt: new Date().toISOString(),
          status: 'completed' as const,
          performance: totalReturn
        }

        // Save to localStorage
        const existing = JSON.parse(localStorage.getItem('factsheets') || '[]')
        existing.unshift(newFactsheet)
        localStorage.setItem('factsheets', JSON.stringify(existing))

        // Show the factsheet
        setFactsheetData(factsheetData)
        setShowFactsheet(true)
        
        // Reset form
        setUploadedFile(null)
        setSelectedStrategy(null)
      }
      reader.readAsText(uploadedFile)
    }
  }

  const strategies = {
    'smart-arbitrage': {
      name: 'SmartArb',
      description: 'Basis capture between futures/perps and spot pairs for low-volatility yield. Delta-neutral strategy focused on basis trading opportunities.'
    },
    'descartes': {
      name: 'Descartes',
      description: 'Cross-sectional momentum machine-learning long/short strategy in liquid markets, refreshed daily to capture cross-sectional momentum. Directional exposure when model confidence is high.'
    },
    'vision': {
      name: 'Vision',
      description: 'Selective long exposure with dynamic hedging powered by factor & sentiment regimes. Discretionary trading on crypto with systematic risk management.'
    }
  }

  return (
    <div className="min-h-screen dfi-gradient">
      {/* Header */}
      <header className="border-b border-dfi-border bg-white/80 backdrop-blur-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-6">
            <a href="/admin" className="flex items-center space-x-3 hover:opacity-80 transition-opacity">
              <div className="w-10 h-10 dfi-accent-gradient rounded-lg flex items-center justify-center">
                <span className="text-white font-bold text-xl">DF</span>
              </div>
              <div>
                <h1 className="text-2xl font-bold dfi-text-gradient">DFI Labs</h1>
                <p className="text-dfi-text-light text-sm">Investment Management</p>
              </div>
            </a>
            <a 
              href="/history" 
              className="text-dfi-text-light hover:text-dfi-text transition-colors flex items-center space-x-2"
            >
              <FileText className="w-4 h-4" />
              <span>History</span>
            </a>
          </div>
        </div>
      </header>

      <main className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        {!selectedOption ? (
          // Main Options Selection
          <div className="text-center mb-12">
            <h2 className="text-3xl font-bold dfi-text-gradient mb-4">Choose Your Data Source</h2>
            <p className="text-dfi-text-light mb-8">Select how you want to generate your factsheet</p>
            
            <div className="grid md:grid-cols-2 gap-8">
              <motion.div
                initial={{ opacity: 0, x: -20 }}
                animate={{ opacity: 1, x: 0 }}
                className="dfi-card p-8 cursor-pointer hover:shadow-lg transition-all"
                onClick={() => setSelectedOption('raw')}
              >
                <div className="w-16 h-16 dfi-accent-gradient rounded-xl flex items-center justify-center mx-auto mb-6">
                  <Upload className="w-8 h-8 text-white" />
                </div>
                <h3 className="text-xl font-semibold text-dfi-text mb-4">Raw Data Upload</h3>
                <p className="text-dfi-text-light">Upload your historical performance data using our template</p>
              </motion.div>

              <motion.div
                initial={{ opacity: 0, x: 20 }}
                animate={{ opacity: 1, x: 0 }}
                className="dfi-card p-8 cursor-pointer hover:shadow-lg transition-all"
                onClick={() => setSelectedOption('live')}
              >
                <div className="w-16 h-16 dfi-accent-gradient rounded-xl flex items-center justify-center mx-auto mb-6">
                  <Database className="w-8 h-8 text-white" />
                </div>
                <h3 className="text-xl font-semibold text-dfi-text mb-4">Live Account</h3>
                <p className="text-dfi-text-light">Connect to your live trading account for real-time data</p>
              </motion.div>
            </div>
          </div>
        ) : selectedOption === 'raw' ? (
          // Raw Data Upload Interface
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="space-y-8"
          >
            <div className="text-center">
              <h2 className="text-2xl font-bold dfi-text-gradient mb-4">Upload Your Data</h2>
              <p className="text-dfi-text-light">Download the template, fill it with your data, and upload it here</p>
            </div>

            {/* Template Download */}
            <div className="dfi-card p-6">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-lg font-semibold text-dfi-text mb-2">Data Template</h3>
                  <p className="text-dfi-text-light text-sm">Download the CSV template to see the required data format</p>
                </div>
                <button
                  onClick={generateTemplate}
                  className="dfi-button-primary flex items-center space-x-2"
                >
                  <Download className="w-4 h-4" />
                  <span>Download Template</span>
                </button>
              </div>
            </div>

            {/* File Upload */}
            <div className="dfi-card p-6">
              <h3 className="text-lg font-semibold text-dfi-text mb-4">Upload Your Data</h3>
              <div 
                className="border-2 border-dashed border-dfi-border rounded-lg p-8 text-center hover:border-dfi-accent transition-colors"
                onDrop={handleDrop}
                onDragOver={handleDragOver}
              >
                <input
                  type="file"
                  accept=".csv"
                  onChange={handleFileUpload}
                  className="hidden"
                  id="file-upload"
                />
                <div
                  onClick={() => document.getElementById('file-upload')?.click()}
                  className="cursor-pointer flex flex-col items-center space-y-4"
                >
                  <Upload className="w-12 h-12 text-dfi-text-light" />
                  <div>
                    <p className="text-dfi-text font-medium">
                      {uploadedFile ? uploadedFile.name : 'Click to upload CSV file or drag & drop'}
                    </p>
                    <p className="text-dfi-text-light text-sm">CSV format only</p>
                  </div>
                </div>
              </div>
            </div>

            {/* Strategy Selection */}
            {uploadedFile && (
              <div className="dfi-card p-6">
                <h3 className="text-lg font-semibold text-dfi-text mb-4">Choose Strategy</h3>
                <div className="grid gap-4">
                  {Object.entries(strategies).map(([key, strategy]) => (
                    <div
                      key={key}
                      className={`p-4 border rounded-lg cursor-pointer transition-all ${
                        selectedStrategy === key
                          ? 'border-dfi-accent bg-dfi-accent/5'
                          : 'border-dfi-border hover:border-dfi-accent/50'
                      }`}
                      onClick={() => setSelectedStrategy(key as any)}
                    >
                      <h4 className="font-semibold text-dfi-text">{strategy.name}</h4>
                      <p className="text-dfi-text-light text-sm mt-1">{strategy.description}</p>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Generate Button */}
            {uploadedFile && selectedStrategy && (
              <div className="text-center">
                <button
                  onClick={generateFactsheet}
                  className="dfi-button-primary text-lg px-8 py-4"
                >
                  Generate Factsheet
                </button>
              </div>
            )}

            {/* Back Button */}
            <div className="text-center">
              <button
                onClick={() => {
                  setSelectedOption(null)
                  setUploadedFile(null)
                  setSelectedStrategy(null)
                }}
                className="text-dfi-text-light hover:text-dfi-text transition-colors"
              >
                ← Back to Options
              </button>
            </div>
          </motion.div>
        ) : (
          // Live Account (Placeholder)
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="text-center"
          >
            <div className="w-16 h-16 dfi-accent-gradient rounded-xl flex items-center justify-center mx-auto mb-6">
              <Database className="w-8 h-8 text-white" />
            </div>
            <h2 className="text-2xl font-bold dfi-text-gradient mb-4">Live Account Connection</h2>
            <p className="text-dfi-text-light mb-8">This feature will be available soon</p>
            <button
              onClick={() => setSelectedOption(null)}
              className="text-dfi-text-light hover:text-dfi-text transition-colors"
            >
              ← Back to Options
            </button>
          </motion.div>
        )}
      </main>

      {/* Factsheet Display Modal */}
      {showFactsheet && factsheetData && (
        <FactsheetDisplay
          data={factsheetData}
          onClose={() => setShowFactsheet(false)}
        />
      )}
    </div>
  )
}
