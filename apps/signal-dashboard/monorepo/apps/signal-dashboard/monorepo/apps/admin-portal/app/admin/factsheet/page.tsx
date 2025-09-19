'use client'

import { useState } from 'react'
import Link from 'next/link'

export default function AdminFactsheet() {
  const [selectedOption, setSelectedOption] = useState<string | null>(null)
  const [uploadedFile, setUploadedFile] = useState<File | null>(null)
  const [selectedStrategy, setSelectedStrategy] = useState<string | null>(null)
  const [showFactsheet, setShowFactsheet] = useState(false)
  const [factsheetData, setFactsheetData] = useState<any>(null)

  const strategies = {
    smartarb: {
      name: 'Smart Arbitrage',
      description: 'Cross-exchange arbitrage strategy capturing price discrepancies across multiple cryptocurrency exchanges with automated execution.',
      color: '#3B82F6'
    },
    descartes: {
      name: 'Descartes',
      description: 'Cross-sectional momentum machine-learning long/short strategy in liquid markets, refreshed daily to capture cross-sectional momentum.',
      color: '#8B5CF6'
    },
    vision: {
      name: 'Vision',
      description: 'Multi-timeframe trend-following strategy using advanced pattern recognition and machine learning for directional exposure.',
      color: '#10B981'
    }
  }

  const generateTemplate = () => {
    const csvContent = 'date,performance\n2023-01-01,0.5\n2023-01-02,-0.2\n2023-01-03,1.2\n2023-01-04,0.8\n2023-01-05,-0.5'
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
    } else {
      alert('Please upload a CSV file')
    }
  }

  const handleDrop = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault()
    const file = event.dataTransfer.files[0]
    if (file && file.type === 'text/csv') {
      setUploadedFile(file)
    } else {
      alert('Please upload a CSV file')
    }
  }

  const generateFactsheet = () => {
    if (uploadedFile && selectedStrategy) {
      // Simulate factsheet generation
      const mockData = {
        strategy: selectedStrategy,
        fileName: uploadedFile.name.replace('.csv', ''),
        performance: Math.random() * 20 - 10, // Random performance between -10% and +10%
        sharpeRatio: Math.random() * 2 - 1,
        maxDrawdown: Math.random() * 20,
        totalReturn: Math.random() * 30 - 15,
        volatility: Math.random() * 30 + 10,
        winRate: Math.random() * 30 + 50,
        totalTrades: Math.floor(Math.random() * 200) + 100,
        avgTradeReturn: Math.random() * 2 - 1
      }
      
      setFactsheetData(mockData)
      setShowFactsheet(true)
      
      // Reset form
      setUploadedFile(null)
      setSelectedStrategy(null)
    }
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-6">
            <div className="flex items-center space-x-4">
              <Link href="/admin" className="flex items-center space-x-3 hover:opacity-80 transition-opacity">
                <div className="w-10 h-10 dfi-accent-gradient rounded-lg flex items-center justify-center">
                  <span className="text-white font-bold text-xl">DF</span>
                </div>
                <div>
                  <h1 className="text-2xl font-bold dfi-text-gradient">DFI Labs</h1>
                  <p className="text-dfi-text-light text-sm">Admin Portal</p>
                </div>
              </Link>
            </div>
            <div className="flex items-center space-x-4">
              <Link href="/admin" className="text-sm text-gray-500 hover:text-gray-700">← Back to Dashboard</Link>
              <span className="text-sm text-gray-500">Factsheet Module</span>
            </div>
          </div>
        </div>
      </header>

      {/* Breadcrumb */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
        <nav className="flex" aria-label="Breadcrumb">
          <ol className="flex items-center space-x-4">
            <li>
              <Link href="/admin" className="text-gray-400 hover:text-gray-500">
                <span className="sr-only">Home</span>
                Dashboard
              </Link>
            </li>
            <li>
              <div className="flex items-center">
                <svg className="flex-shrink-0 h-5 w-5 text-gray-300" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M7.293 14.707a1 1 0 010-1.414L10.586 10 7.293 6.707a1 1 0 011.414-1.414l4 4a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0z" clipRule="evenodd" />
                </svg>
                <span className="ml-4 text-sm font-medium text-gray-500">Factsheet Generator</span>
              </div>
            </li>
          </ol>
        </nav>
      </div>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="mb-8">
          <h2 className="text-3xl font-bold text-gray-900 mb-2">Factsheet Generator</h2>
          <p className="text-gray-600">Generate professional investment factsheets for your strategies</p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          {/* Left Column - Configuration */}
          <div className="space-y-6">
            {/* Data Source Selection */}
            <div className="bg-white rounded-lg shadow p-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">Choose Your Data Source</h3>
              <div className="space-y-4">
                <button
                  onClick={() => setSelectedOption('raw')}
                  className={`w-full p-4 rounded-lg border-2 text-left transition-all ${
                    selectedOption === 'raw'
                      ? 'border-dfi-accent bg-dfi-accent/5'
                      : 'border-gray-200 hover:border-gray-300'
                  }`}
                >
                  <div className="flex items-center space-x-3">
                    <div className="w-8 h-8 bg-blue-100 rounded-lg flex items-center justify-center">
                      <span className="text-blue-600 font-semibold">📊</span>
                    </div>
                    <div>
                      <h4 className="font-semibold text-gray-900">Raw Data Upload</h4>
                      <p className="text-sm text-gray-600">Upload your historical performance data using our template</p>
                    </div>
                  </div>
                </button>
                
                <button
                  onClick={() => setSelectedOption('live')}
                  className={`w-full p-4 rounded-lg border-2 text-left transition-all ${
                    selectedOption === 'live'
                      ? 'border-dfi-accent bg-dfi-accent/5'
                      : 'border-gray-200 hover:border-gray-300'
                  }`}
                >
                  <div className="flex items-center space-x-3">
                    <div className="w-8 h-8 bg-green-100 rounded-lg flex items-center justify-center">
                      <span className="text-green-600 font-semibold">🔗</span>
                    </div>
                    <div>
                      <h4 className="font-semibold text-gray-900">Live Account</h4>
                      <p className="text-sm text-gray-600">Connect to your live trading account for real-time data</p>
                    </div>
                  </div>
                </button>
              </div>
            </div>

            {/* File Upload */}
            {selectedOption === 'raw' && (
              <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-lg font-semibold text-gray-900 mb-4">Upload Your Data</h3>
                <div className="space-y-4">
                  <button
                    onClick={generateTemplate}
                    className="dfi-button-secondary w-full"
                  >
                    Download CSV Template
                  </button>
                  
                  <div
                    onDrop={handleDrop}
                    onDragOver={(e) => e.preventDefault()}
                    className="border-2 border-dashed border-gray-300 rounded-lg p-6 text-center hover:border-dfi-accent transition-colors"
                  >
                    <input
                      type="file"
                      accept=".csv"
                      onChange={handleFileUpload}
                      className="hidden"
                      id="file-upload"
                    />
                    <label htmlFor="file-upload" className="cursor-pointer">
                      {uploadedFile ? (
                        <div className="text-green-600">
                          <span className="text-2xl">✅</span>
                          <p className="mt-2 font-medium">{uploadedFile.name}</p>
                        </div>
                      ) : (
                        <div className="text-gray-500">
                          <span className="text-2xl">📁</span>
                          <p className="mt-2">Drop your CSV file here or click to browse</p>
                        </div>
                      )}
                    </label>
                  </div>
                </div>
              </div>
            )}

            {/* Strategy Selection */}
            {uploadedFile && (
              <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-lg font-semibold text-gray-900 mb-4">Select Strategy</h3>
                <div className="space-y-3">
                  {Object.entries(strategies).map(([key, strategy]) => (
                    <button
                      key={key}
                      onClick={() => setSelectedStrategy(key)}
                      className={`w-full p-4 rounded-lg border-2 text-left transition-all ${
                        selectedStrategy === key
                          ? 'border-dfi-accent bg-dfi-accent/5'
                          : 'border-gray-200 hover:border-gray-300'
                      }`}
                    >
                      <div className="flex items-center space-x-3">
                        <div 
                          className="w-4 h-4 rounded-full"
                          style={{ backgroundColor: strategy.color }}
                        ></div>
                        <div>
                          <h4 className="font-semibold text-gray-900">{strategy.name}</h4>
                          <p className="text-sm text-gray-600">{strategy.description}</p>
                        </div>
                      </div>
                    </button>
                  ))}
                </div>
              </div>
            )}

            {/* Generate Button */}
            {uploadedFile && selectedStrategy && (
              <button
                onClick={generateFactsheet}
                className="dfi-button-primary w-full py-4 text-lg"
              >
                Generate Factsheet
              </button>
            )}
          </div>

          {/* Right Column - Preview/Info */}
          <div className="space-y-6">
            <div className="bg-white rounded-lg shadow p-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">Module Information</h3>
              <div className="space-y-4">
                <div className="flex items-start space-x-3">
                  <div className="p-2 bg-blue-100 rounded-lg">
                    <span className="text-blue-600">📊</span>
                  </div>
                  <div>
                    <h4 className="font-medium text-gray-900">Professional Factsheets</h4>
                    <p className="text-sm text-gray-600">Generate institutional-grade factsheets with real-time data and sophisticated design.</p>
                  </div>
                </div>
                <div className="flex items-start space-x-3">
                  <div className="p-2 bg-green-100 rounded-lg">
                    <span className="text-green-600">🎨</span>
                  </div>
                  <div>
                    <h4 className="font-medium text-gray-900">Beautiful Design</h4>
                    <p className="text-sm text-gray-600">Apple-style elegant PDFs with performance charts, team information, and professional layout.</p>
                  </div>
                </div>
                <div className="flex items-start space-x-3">
                  <div className="p-2 bg-purple-100 rounded-lg">
                    <span className="text-purple-600">📈</span>
                  </div>
                  <div>
                    <h4 className="font-medium text-gray-900">Real-time Data</h4>
                    <p className="text-sm text-gray-600">Integrated with live BTC and Cash benchmark data for accurate performance comparisons.</p>
                  </div>
                </div>
              </div>
            </div>

            {showFactsheet && factsheetData && (
              <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-lg font-semibold text-gray-900 mb-4">Factsheet Generated!</h3>
                <div className="space-y-3">
                  <div className="flex justify-between">
                    <span className="text-gray-600">Strategy:</span>
                    <span className="font-medium">{strategies[factsheetData.strategy as keyof typeof strategies]?.name}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">File:</span>
                    <span className="font-medium">{factsheetData.fileName}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Performance:</span>
                    <span className={`font-medium ${factsheetData.performance >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                      {factsheetData.performance.toFixed(2)}%
                    </span>
                  </div>
                  <div className="pt-4">
                    <button className="dfi-button-primary w-full">
                      Download PDF Factsheet
                    </button>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </main>
    </div>
  )
}
