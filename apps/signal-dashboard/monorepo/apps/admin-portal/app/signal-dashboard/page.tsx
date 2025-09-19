'use client'

import { useState, useEffect } from 'react'
import { motion } from 'framer-motion'
import { 
  ArrowLeft,
  FileText,
  TrendingUp,
  TrendingDown,
  DollarSign,
  Activity,
  BarChart3,
  RefreshCw
} from 'lucide-react'

export default function SignalDashboardPage() {
  const [isLoading, setIsLoading] = useState(true)
  const [portfolioData, setPortfolioData] = useState({
    portfolioValue: 1000000,
    dailyPnL: 0,
    totalPnL: 0,
    totalNotional: 0,
    positions: 0
  })

  useEffect(() => {
    // Simulate loading data
    const timer = setTimeout(() => {
      setIsLoading(false)
    }, 2000)

    return () => clearTimeout(timer)
  }, [])

  const longPositions = [
    { symbol: 'BTC/USD', side: 'Long', weight: 45.2, notional: 452000, entryPrice: 43250, currentPrice: 43500, pnl: 2500, pnlPercent: 0.55 },
    { symbol: 'ETH/USD', side: 'Long', weight: 32.1, notional: 321000, entryPrice: 2650, currentPrice: 2680, pnl: 3000, pnlPercent: 0.93 },
    { symbol: 'SOL/USD', side: 'Long', weight: 22.7, notional: 227000, entryPrice: 98.5, currentPrice: 101.2, pnl: 2700, pnlPercent: 1.19 }
  ]

  const shortPositions = [
    { symbol: 'ADA/USD', side: 'Short', weight: -15.3, notional: -153000, entryPrice: 0.485, currentPrice: 0.478, pnl: 2100, pnlPercent: 1.37 },
    { symbol: 'DOT/USD', side: 'Short', weight: -8.9, notional: -89000, entryPrice: 6.85, currentPrice: 6.72, pnl: 1300, pnlPercent: 1.46 }
  ]

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-6">
            <div className="flex items-center space-x-4">
              <a 
                href="/admin" 
                className="flex items-center space-x-2 text-gray-600 hover:text-gray-900 transition-colors"
              >
                <ArrowLeft className="w-5 h-5" />
                <span>Back to Admin</span>
              </a>
              <div className="w-12 h-12 bg-gradient-to-r from-green-600 to-emerald-600 rounded-xl flex items-center justify-center">
                <BarChart3 className="w-6 h-6 text-white" />
              </div>
              <div>
                <h1 className="text-3xl font-bold bg-gradient-to-r from-green-600 to-emerald-600 bg-clip-text text-transparent">
                  Strategy Monitor
                </h1>
                <p className="text-gray-600 text-sm">In-house simple strategy tracking & monitoring</p>
              </div>
            </div>
            <div className="flex items-center space-x-4">
              <div className="text-right">
                <p className="text-sm font-medium text-gray-900">UTC / Paris</p>
                <p className="text-xs text-gray-500">{new Date().toLocaleTimeString()}</p>
              </div>
              <button className="flex items-center space-x-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors">
                <RefreshCw className="w-4 h-4" />
                <span>Refresh</span>
              </button>
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Data Source */}
        <div className="mb-8">
          <div className="flex items-center space-x-4">
            <div className="flex items-center space-x-2">
              <FileText className="w-5 h-5 text-gray-600" />
              <span className="text-gray-700 font-medium">Data:</span>
              <button className="text-blue-600 hover:text-blue-700 font-medium">
                Click to show file
              </button>
            </div>
            {isLoading && (
              <div className="flex items-center space-x-2 text-gray-500">
                <RefreshCw className="w-4 h-4 animate-spin" />
                <span>Loading...</span>
              </div>
            )}
          </div>
        </div>

        {/* Portfolio Stats */}
        <div className="grid grid-cols-1 md:grid-cols-5 gap-6 mb-8">
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="bg-white rounded-xl shadow-lg p-6 border border-gray-200"
          >
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-medium text-gray-600">Portfolio Value</h3>
              <DollarSign className="w-5 h-5 text-gray-400" />
            </div>
            <div className="text-2xl font-bold text-gray-900">
              ${portfolioData.portfolioValue.toLocaleString()}
            </div>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.1 }}
            className="bg-white rounded-xl shadow-lg p-6 border border-gray-200"
          >
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-medium text-gray-600">Daily P&L</h3>
              <Activity className="w-5 h-5 text-gray-400" />
            </div>
            <div className="text-2xl font-bold text-gray-900">
              ${portfolioData.dailyPnL.toLocaleString()}
            </div>
            <div className="text-sm text-gray-500">(0.00%)</div>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            className="bg-white rounded-xl shadow-lg p-6 border border-gray-200"
          >
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-medium text-gray-600">Total P&L Since Inception</h3>
              <TrendingUp className="w-5 h-5 text-green-500" />
            </div>
            <div className="text-2xl font-bold text-green-600">
              ${portfolioData.totalPnL.toLocaleString()}
            </div>
            <div className="text-sm text-gray-500">(0.00%)</div>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.3 }}
            className="bg-white rounded-xl shadow-lg p-6 border border-gray-200"
          >
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-medium text-gray-600">Total Notional at Entry</h3>
              <DollarSign className="w-5 h-5 text-gray-400" />
            </div>
            <div className="text-2xl font-bold text-gray-900">
              ${portfolioData.totalNotional.toLocaleString()}
            </div>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.4 }}
            className="bg-white rounded-xl shadow-lg p-6 border border-gray-200"
          >
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-medium text-gray-600">Positions</h3>
              <BarChart3 className="w-5 h-5 text-gray-400" />
            </div>
            <div className="text-2xl font-bold text-gray-900">
              {portfolioData.positions}
            </div>
          </motion.div>
        </div>

        {/* Portfolio Value Evolution */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.5 }}
          className="bg-white rounded-xl shadow-lg p-6 mb-8 border border-gray-200"
        >
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Portfolio Value Evolution</h3>
          <p className="text-gray-600 mb-4">Daily portfolio value tracking since inception</p>
          <div className="h-64 bg-gray-50 rounded-lg flex items-center justify-center">
            <div className="text-center">
              <BarChart3 className="w-12 h-12 text-gray-400 mx-auto mb-2" />
              <p className="text-gray-500">Chart visualization coming soon</p>
            </div>
          </div>
        </motion.div>

        {/* Positions Tables */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          {/* Long Positions */}
          <motion.div
            initial={{ opacity: 0, x: -20 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ delay: 0.6 }}
            className="bg-white rounded-xl shadow-lg border border-gray-200"
          >
            <div className="p-6 border-b border-gray-200">
              <h3 className="text-lg font-semibold text-gray-900 mb-2">Long Positions</h3>
              <p className="text-sm text-gray-600">
                Total Notional at Entry: ${longPositions.reduce((sum, pos) => sum + pos.notional, 0).toLocaleString()}
              </p>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Symbol</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Side</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Weight %</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Notional $</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Entry Price</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Current Price</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">P&L $</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">P&L %</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {longPositions.map((position, index) => (
                    <tr key={index} className="hover:bg-gray-50">
                      <td className="px-4 py-3 text-sm font-medium text-gray-900">{position.symbol}</td>
                      <td className="px-4 py-3 text-sm text-gray-900">{position.side}</td>
                      <td className="px-4 py-3 text-sm text-gray-900">{position.weight}%</td>
                      <td className="px-4 py-3 text-sm text-gray-900">${position.notional.toLocaleString()}</td>
                      <td className="px-4 py-3 text-sm text-gray-900">${position.entryPrice.toLocaleString()}</td>
                      <td className="px-4 py-3 text-sm text-gray-900">${position.currentPrice.toLocaleString()}</td>
                      <td className="px-4 py-3 text-sm font-medium text-green-600">+${position.pnl.toLocaleString()}</td>
                      <td className="px-4 py-3 text-sm font-medium text-green-600">+{position.pnlPercent}%</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </motion.div>

          {/* Short Positions */}
          <motion.div
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ delay: 0.7 }}
            className="bg-white rounded-xl shadow-lg border border-gray-200"
          >
            <div className="p-6 border-b border-gray-200">
              <h3 className="text-lg font-semibold text-gray-900 mb-2">Short Positions</h3>
              <p className="text-sm text-gray-600">
                Total Notional at Entry: ${Math.abs(shortPositions.reduce((sum, pos) => sum + pos.notional, 0)).toLocaleString()}
              </p>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Symbol</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Side</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Weight %</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Notional $</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Entry Price</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Current Price</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">P&L $</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">P&L %</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {shortPositions.map((position, index) => (
                    <tr key={index} className="hover:bg-gray-50">
                      <td className="px-4 py-3 text-sm font-medium text-gray-900">{position.symbol}</td>
                      <td className="px-4 py-3 text-sm text-gray-900">{position.side}</td>
                      <td className="px-4 py-3 text-sm text-gray-900">{position.weight}%</td>
                      <td className="px-4 py-3 text-sm text-gray-900">${Math.abs(position.notional).toLocaleString()}</td>
                      <td className="px-4 py-3 text-sm text-gray-900">${position.entryPrice.toLocaleString()}</td>
                      <td className="px-4 py-3 text-sm text-gray-900">${position.currentPrice.toLocaleString()}</td>
                      <td className="px-4 py-3 text-sm font-medium text-green-600">+${position.pnl.toLocaleString()}</td>
                      <td className="px-4 py-3 text-sm font-medium text-green-600">+{position.pnlPercent}%</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </motion.div>
        </div>

        {/* Monitoring Status */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.8 }}
          className="mt-8 text-center"
        >
          <div className="inline-flex items-center space-x-2 px-4 py-2 bg-green-100 text-green-800 rounded-full">
            <RefreshCw className="w-4 h-4 animate-spin" />
            <span className="text-sm font-medium">Monitoring CSV files...</span>
          </div>
        </motion.div>
      </main>
    </div>
  )
}
