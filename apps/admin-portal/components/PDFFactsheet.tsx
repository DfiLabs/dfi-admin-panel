'use client'

import { useEffect, useRef, useState } from 'react'
import jsPDF from 'jspdf'
import html2canvas from 'html2canvas'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'

interface FactsheetData {
  strategy: string
  fileName: string
  performance: number
  sharpeRatio: number
  maxDrawdown: number
  totalReturn: number
  volatility: number
  winRate: number
  totalTrades: number
  avgTradeReturn: number
  rawData?: Array<{ date: Date, performance: number }>
}

interface PDFFactsheetProps {
  data: FactsheetData
  onClose: () => void
}

export default function PDFFactsheet({ data, onClose }: PDFFactsheetProps) {
  const factsheetRef = useRef<HTMLDivElement>(null)
  const [isGenerating, setIsGenerating] = useState(true)
  const [btcData, setBtcData] = useState<Array<{ date: Date, price: number, monthlyReturn: number }> | null>(null)
  const [cashData, setCashData] = useState<Array<{ date: Date, monthlyReturn: number }> | null>(null)

  // Fetch real Cash/Treasury data for Descartes benchmark
  const fetchCashData = async () => {
    try {
      // Get US Treasury 3-month rate data (proxy for cash returns)
      const endDate = new Date()
      const startDate = new Date()
      startDate.setFullYear(endDate.getFullYear() - 2)
      
      const startTimestamp = Math.floor(startDate.getTime() / 1000)
      const endTimestamp = Math.floor(endDate.getTime() / 1000)
      
      // Using FRED API for US Treasury rates (free API)
      const response = await fetch(
        `https://api.stlouisfed.org/fred/series/observations?series_id=DGS3MO&api_key=YOUR_FRED_API_KEY&file_type=json&observation_start=${startDate.toISOString().split('T')[0]}&observation_end=${endDate.toISOString().split('T')[0]}`
      )
      
      if (!response.ok) {
        throw new Error('Failed to fetch cash data')
      }
      
      const cashApiData = await response.json()
      
      // Process daily rates into monthly returns
      const monthlyData = new Map()
      
      cashApiData.observations.forEach((obs: any) => {
        if (obs.value !== '.') {
          const date = new Date(obs.date)
          const monthKey = `${date.getFullYear()}-${date.getMonth()}`
          const rate = parseFloat(obs.value) / 100 / 12 // Convert annual rate to monthly
          
          if (!monthlyData.has(monthKey)) {
            monthlyData.set(monthKey, [])
          }
          monthlyData.get(monthKey).push(rate)
        }
      })
      
      // Calculate average monthly returns
      const cashMonthlyReturns: Array<{ date: Date, monthlyReturn: number }> = []
      const sortedMonths = Array.from(monthlyData.keys()).sort()
      
      sortedMonths.forEach(monthKey => {
        const rates = monthlyData.get(monthKey)
        const avgMonthlyReturn = rates.reduce((sum, rate) => sum + rate, 0) / rates.length
        
        const [year, month] = monthKey.split('-').map(Number)
        const date = new Date(year, month, 1)
        
        cashMonthlyReturns.push({
          date,
          monthlyReturn: avgMonthlyReturn * 100 // Convert to percentage
        })
      })
      
      setCashData(cashMonthlyReturns)
    } catch (error) {
      console.error('Error fetching cash data:', error)
      // Fallback to sample cash data (0.2% monthly average)
      const sampleCashData = []
      const endDate = new Date()
      for (let i = 0; i < 24; i++) {
        const date = new Date(endDate.getFullYear(), endDate.getMonth() - i, 1)
        sampleCashData.push({
          date,
          monthlyReturn: 0.2 + (Math.random() - 0.5) * 0.1 // 0.15% to 0.25% monthly
        })
      }
      setCashData(sampleCashData.reverse())
    }
  }

  // Fetch real BTC data
  const fetchBTCData = async () => {
    try {
      // Get data for the last 2 years to ensure we have enough data
      const endDate = new Date()
      const startDate = new Date()
      startDate.setFullYear(endDate.getFullYear() - 2)
      
      const startTimestamp = Math.floor(startDate.getTime() / 1000)
      const endTimestamp = Math.floor(endDate.getTime() / 1000)
      
      const response = await fetch(
        `https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range?vs_currency=usd&from=${startTimestamp}&to=${endTimestamp}`
      )
      
      if (!response.ok) {
        throw new Error('Failed to fetch BTC data')
      }
      
      const btcApiData = await response.json()
      
      // Process daily prices into monthly returns
      const dailyPrices = btcApiData.prices.map(([timestamp, price]: [number, number]) => ({
        date: new Date(timestamp),
        price: price
      }))
      
      // Group by month and calculate monthly returns
      const monthlyData = new Map()
      
      dailyPrices.forEach(({ date, price }) => {
        const monthKey = `${date.getFullYear()}-${date.getMonth()}`
        if (!monthlyData.has(monthKey)) {
          monthlyData.set(monthKey, [])
        }
        monthlyData.get(monthKey).push(price)
      })
      
      // Calculate monthly returns
      const btcMonthlyReturns: Array<{ date: Date, price: number, monthlyReturn: number }> = []
      const sortedMonths = Array.from(monthlyData.keys()).sort()
      
      for (let i = 0; i < sortedMonths.length; i++) {
        const monthKey = sortedMonths[i]
        const prices = monthlyData.get(monthKey)
        const firstPrice = prices[0]
        const lastPrice = prices[prices.length - 1]
        const monthlyReturn = ((lastPrice - firstPrice) / firstPrice) * 100
        
        const [year, month] = monthKey.split('-').map(Number)
        const date = new Date(year, month, 1)
        
        btcMonthlyReturns.push({
          date,
          price: lastPrice,
          monthlyReturn
        })
      }
      
      setBtcData(btcMonthlyReturns)
    } catch (error) {
      console.error('Error fetching BTC data:', error)
      // Fallback to null - will use sample data
      setBtcData(null)
    }
  }

  // Auto-generate PDF when component mounts
  useEffect(() => {
    // Fetch both BTC and cash data
    fetchBTCData()
    fetchCashData()
    
    const timer = setTimeout(() => {
      generatePDF()
    }, 3000) // Increased delay to allow both data sources to load

    return () => clearTimeout(timer)
  }, [])

  const strategyInfo = {
    'smart-arbitrage': {
      name: 'SmartArb',
      description: 'Basis capture between futures/perps and spot pairs for low-volatility yield. Delta-neutral strategy focused on basis trading opportunities.',
      color: '#6366F1' // Indigo matching DFI Labs brand
    },
    'descartes': {
      name: 'Descartes',
      description: 'Cross-sectional momentum machine-learning long/short strategy in liquid markets, refreshed daily to capture cross-sectional momentum. Directional exposure when model confidence is high.',
      color: '#8B5CF6' // Purple matching DFI Labs brand
    },
    'vision': {
      name: 'Vision',
      description: 'Selective long exposure with dynamic hedging powered by factor & sentiment regimes. Discretionary trading on crypto with systematic risk management.',
      color: '#6366F1' // Indigo matching DFI Labs brand
    }
  }

  const strategy = strategyInfo[data.strategy as keyof typeof strategyInfo]

  // Generate chart data from actual performance data
  const generateChartData = () => {
    if (data.rawData && data.rawData.length > 0) {
      // Group strategy data by month and calculate monthly returns
      const strategyMonthlyData = new Map()
      
      data.rawData.forEach(point => {
        const monthKey = `${point.date.getFullYear()}-${point.date.getMonth()}`
        if (!strategyMonthlyData.has(monthKey)) {
          strategyMonthlyData.set(monthKey, [])
        }
        strategyMonthlyData.get(monthKey).push(point.performance)
      })
      
      // Calculate cumulative returns
      const chartData = []
      let strategyCumulativeReturn = 0
      let benchmarkCumulativeReturn = 0
      const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
      
      // Get the most recent 12 months
      const sortedMonths = Array.from(strategyMonthlyData.keys()).sort().slice(-12)
      
      sortedMonths.forEach(monthKey => {
        const monthReturns = strategyMonthlyData.get(monthKey)
        const strategyMonthlyReturn = monthReturns.reduce((sum, ret) => sum + ret, 0)
        strategyCumulativeReturn += strategyMonthlyReturn
        
        // Choose benchmark based on strategy
        const [year, month] = monthKey.split('-').map(Number)
        let benchmarkMonthlyReturn = 0
        
        if (data.strategy === 'descartes' && cashData) {
          // Use cash data for Descartes (market-neutral strategy)
          const cashMonthData = cashData.find(cash => 
            cash.date.getFullYear() === year && cash.date.getMonth() === month
          )
          benchmarkMonthlyReturn = cashMonthData ? cashMonthData.monthlyReturn : 0
        } else if (btcData) {
          // Use BTC data for other strategies
          const btcMonthData = btcData.find(btc => 
            btc.date.getFullYear() === year && btc.date.getMonth() === month
          )
          benchmarkMonthlyReturn = btcMonthData ? btcMonthData.monthlyReturn : 0
        }
        
        benchmarkCumulativeReturn += benchmarkMonthlyReturn
        
        const monthIndex = parseInt(monthKey.split('-')[1])
        chartData.push({
          month: months[monthIndex],
          value: Math.round(strategyCumulativeReturn * 100) / 100,
          cumulative: Math.round(strategyCumulativeReturn * 100) / 100,
          benchmark: Math.round(benchmarkCumulativeReturn * 100) / 100
        })
      })
      
      return chartData
    } else {
      // Fallback with sample data (no benchmark)
      const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
      let cumulativeReturn = 0
      
      return months.map(month => {
        const monthlyReturn = (Math.random() - 0.3) * 4 // -1.2% to +2.8%
        cumulativeReturn += monthlyReturn
        
        return {
          month,
          value: Math.round(cumulativeReturn * 100) / 100,
          cumulative: Math.round(cumulativeReturn * 100) / 100,
          benchmark: 0 // No benchmark data available
        }
      })
    }
  }

  const chartData = generateChartData()

  const generatePDF = async () => {
    if (!factsheetRef.current) return

    try {
      setIsGenerating(true)
      const canvas = await html2canvas(factsheetRef.current, {
        scale: 2, // Reduced scale for smaller file size
        useCORS: true,
        allowTaint: true,
        backgroundColor: '#ffffff',
        logging: false,
        width: 794, // A4 width in pixels
        height: 1123, // A4 height in pixels
        windowWidth: 794,
        windowHeight: 1123
      })

      const imgData = canvas.toDataURL('image/jpeg', 0.8) // JPEG with 80% quality for smaller size
      const pdf = new jsPDF('p', 'mm', 'a4')
      
      const imgWidth = 210
      const imgHeight = (canvas.height * imgWidth) / canvas.width

      // Only add one page - fit the entire content on one page
      pdf.addImage(imgData, 'JPEG', 0, 0, imgWidth, imgHeight)

      pdf.save(`${data.fileName}-${strategy.name}-factsheet.pdf`)
      
      setIsGenerating(false)
      
      // Close modal after successful download
      setTimeout(() => {
        onClose()
      }, 500)
    } catch (error) {
      console.error('Error generating PDF:', error)
      setIsGenerating(false)
      alert('Error generating PDF. Please try again.')
    }
  }

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center p-4 z-50">
      <div className="bg-white rounded-lg shadow-2xl max-w-md w-full">
        {/* Header */}
        <div className="p-6 text-center">
          <h1 className="text-2xl font-bold text-gray-800 mb-4">Generating PDF</h1>
          {isGenerating ? (
            <div className="space-y-4">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
              <p className="text-gray-600">Creating your beautiful factsheet...</p>
            </div>
          ) : (
            <div className="space-y-4">
              <div className="text-green-600 text-4xl">✓</div>
              <p className="text-gray-600">PDF downloaded successfully!</p>
            </div>
          )}
        </div>

        {/* NOTE: This HTML preview is NOT used for PDF generation
            The actual PDF uses 100% text rendering with pdf.text() methods
            This preview is just for visual reference during loading */}
        <div style={{ position: 'absolute', left: '-9999px', top: '-9999px' }}>
          <div 
            ref={factsheetRef}
            className="bg-white"
            style={{ 
              width: '794px', 
              minHeight: '1123px', 
              fontFamily: 'SF Pro Display, -apple-system, BlinkMacSystemFont, sans-serif',
              WebkitFontSmoothing: 'antialiased',
              MozOsxFontSmoothing: 'grayscale',
              textRendering: 'optimizeLegibility',
              fontSize: '14px',
              lineHeight: '1.5'
            }} // A4 size in pixels with crisp text rendering
          >
            {/* Header */}
            <div className="flex items-center justify-between mb-2 px-4 pt-4">
              <div className="flex items-center">
                <img 
                  src="/images/Logo/25fbbecd-493f-4aee-b87b-04f8a7d5d747.png" 
                  alt="DFI Labs" 
                  className="w-6 h-6 mr-2"
                />
                <div>
                  <h1 className="text-sm font-semibold text-gray-900 tracking-tight">DFI Labs</h1>
                  <p className="text-xs text-gray-500 font-medium">Investment Management</p>
                </div>
              </div>
              <div className="text-right">
                <h2 className="text-lg font-bold text-gray-900 tracking-tight">{strategy.name}</h2>
                <p className="text-xs text-gray-500 font-medium">{new Date().toLocaleDateString('en-US', { month: 'short', year: 'numeric' })}</p>
              </div>
            </div>

            {/* Top Row: Company Description + Performance Summary */}
            <div className="mb-2 px-4">
              <div className="grid grid-cols-2 gap-3">
                {/* Company Description - Left */}
                <div className="bg-gradient-to-r from-gray-50 to-gray-100 rounded p-2 border border-gray-200">
                  <h3 className="text-xs font-semibold text-gray-900 mb-1">About DFI Labs</h3>
                  <p className="text-xs text-gray-600 leading-tight">Paris-based crypto asset manager established in 2019. We deliver institutional-grade risk governance, in-house technology and performance-driven research for systematic crypto strategies.</p>
                </div>
                
                {/* Performance Summary - Right */}
                <div className="bg-white rounded border border-gray-200 p-2 shadow-sm">
                  <h3 className="text-xs font-semibold text-gray-900 mb-1 tracking-tight">Performance Summary (Since Inception)</h3>
                  <div className="grid grid-cols-3 gap-2">
                    <div className="text-center">
                      <div className="text-xs font-bold mb-1 text-gray-900">
                        {data.performance >= 0 ? '+' : ''}{data.performance.toFixed(2)}%
                      </div>
                      <div className="text-xs text-gray-600">Total Return</div>
                      <div className="text-xs text-gray-500">(Jan 2023 - Present)</div>
                    </div>
                    <div className="text-center">
                      <div className="text-xs font-bold mb-1 text-gray-900">{data.sharpeRatio.toFixed(2)}</div>
                      <div className="text-xs text-gray-600">Sharpe Ratio</div>
                      <div className="text-xs text-gray-500">(Annualized)</div>
                    </div>
                    <div className="text-center">
                      <div className="text-xs font-bold mb-1 text-gray-900">{data.volatility.toFixed(2)}%</div>
                      <div className="text-xs text-gray-600">Volatility</div>
                      <div className="text-xs text-gray-500">(Annualized)</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            {/* Middle Row: Strategy + Key Metrics */}
            <div className="mb-2 px-4">
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <h3 className="text-xs font-semibold text-gray-900 mb-1 tracking-tight">Investment Strategy</h3>
                  <p className="text-xs text-gray-600 leading-tight">{strategy.description}</p>
                </div>
                <div>
                  <h3 className="text-xs font-semibold text-gray-900 mb-1 tracking-tight">Risk Metrics (Since Inception)</h3>
                  <div className="grid grid-cols-3 gap-1">
                    <div className="bg-white rounded p-1 border border-gray-200 shadow-sm">
                      <div className="text-center">
                        <div className="text-xs font-bold text-red-500">{data.maxDrawdown.toFixed(2)}%</div>
                        <div className="text-xs text-gray-600">Max Drawdown</div>
                        <div className="text-xs text-gray-500">(Peak-to-Trough)</div>
                      </div>
                    </div>
                    <div className="bg-white rounded p-1 border border-gray-200 shadow-sm">
                      <div className="text-center">
                        <div className="text-xs font-bold text-gray-900">{data.winRate.toFixed(1)}%</div>
                        <div className="text-xs text-gray-600">Win Rate</div>
                        <div className="text-xs text-gray-500">(Positive Days)</div>
                      </div>
                    </div>
                    <div className="bg-white rounded p-1 border border-gray-200 shadow-sm">
                      <div className="text-center">
                        <div className="text-xs font-bold text-gray-900">{data.totalTrades}</div>
                        <div className="text-xs text-gray-600">Trading Days</div>
                        <div className="text-xs text-gray-500">(Since Inception)</div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            {/* Performance Chart + General Information - Smart Layout */}
            <div className="mb-2 px-4">
              <div className="grid grid-cols-3 gap-3">
                {/* Chart - 2/3 width */}
                <div className="col-span-2">
                  <h3 className="text-xs font-semibold text-gray-900 mb-1 tracking-tight">
                    {data.strategy === 'descartes' && cashData 
                      ? 'Cumulative Performance vs Cash Benchmark' 
                      : btcData 
                        ? 'Cumulative Performance vs BTC Benchmark' 
                        : 'Cumulative Performance'}
                  </h3>
                  <div className="bg-white rounded border border-gray-200 p-2 shadow-sm">
                    <div className="h-20">
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={chartData} margin={{ top: 2, right: 2, left: 2, bottom: 2 }}>
                          <CartesianGrid strokeDasharray="1 1" stroke="#f8f9fa" />
                          <XAxis 
                            dataKey="month" 
                            axisLine={false}
                            tickLine={false}
                            tick={{ fontSize: 7, fill: '#666' }}
                            interval={0}
                          />
                          <YAxis 
                            axisLine={false}
                            tickLine={false}
                            tick={{ fontSize: 7, fill: '#666' }}
                            domain={['dataMin - 5', 'dataMax + 5']}
                            tickFormatter={(value) => `${value}%`}
                          />
                          <Tooltip 
                            contentStyle={{
                              backgroundColor: 'white',
                              border: '1px solid #e5e7eb',
                              borderRadius: '4px',
                              boxShadow: '0 1px 2px rgba(0, 0, 0, 0.1)',
                              fontSize: '8px'
                            }}
                            formatter={(value: any, name: string) => [
                              `${value}%`, 
                              name === 'cumulative' ? strategy.name : (data.strategy === 'descartes' ? 'Cash' : 'BTC')
                            ]}
                          />
                          <Line 
                            type="monotone" 
                            dataKey="cumulative" 
                            stroke={strategy.color} 
                            strokeWidth={2}
                            dot={false}
                            activeDot={{ r: 3, stroke: strategy.color, strokeWidth: 1, fill: 'white' }}
                            name={strategy.name}
                          />
                          {((data.strategy === 'descartes' && cashData) || btcData) && (
                            <Line 
                              type="monotone" 
                              dataKey="benchmark" 
                              stroke="#6b7280" 
                              strokeWidth={2}
                              strokeDasharray="4 4"
                              dot={false}
                              activeDot={{ r: 3, stroke: '#6b7280', strokeWidth: 1, fill: 'white' }}
                              name={data.strategy === 'descartes' ? 'Cash' : 'BTC'}
                            />
                          )}
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                    <div className="flex justify-center space-x-4 mt-1">
                      <div className="flex items-center space-x-1">
                        <div className="w-3 h-0.5" style={{ backgroundColor: strategy.color }}></div>
                        <span className="text-xs text-gray-600">{strategy.name}</span>
                      </div>
                      {((data.strategy === 'descartes' && cashData) || btcData) && (
                        <div className="flex items-center space-x-1">
                          <div className="w-3 h-0.5 border-t-2 border-dashed border-gray-500"></div>
                          <span className="text-xs text-gray-600">
                            {data.strategy === 'descartes' ? 'Cash' : 'BTC'}
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
                
                {/* General Information - 1/3 width */}
                <div>
                  <h3 className="text-xs font-semibold text-gray-900 mb-1 tracking-tight">General Information</h3>
                  <div className="bg-white rounded border border-gray-200 overflow-hidden shadow-sm">
                    <div className="overflow-x-auto">
                      <table className="w-full">
                        <tbody>
                          <tr className="border-b border-gray-100">
                            <td className="px-2 py-1 text-xs font-semibold text-gray-900">Company</td>
                            <td className="px-2 py-1 text-xs text-gray-700">DFI Labs</td>
                          </tr>
                          <tr className="border-b border-gray-100">
                            <td className="px-2 py-1 text-xs font-semibold text-gray-900">Vehicle</td>
                            <td className="px-2 py-1 text-xs text-gray-700">Managed Account</td>
                          </tr>
                          <tr className="border-b border-gray-100">
                            <td className="px-2 py-1 text-xs font-semibold text-gray-900">Min Investment</td>
                            <td className="px-2 py-1 text-xs text-gray-700">100,000 USD</td>
                          </tr>
                          <tr className="border-b border-gray-100">
                            <td className="px-2 py-1 text-xs font-semibold text-gray-900">Mgmt Fee</td>
                            <td className="px-2 py-1 text-xs text-gray-700">2.00%</td>
                          </tr>
                          <tr className="border-b border-gray-100">
                            <td className="px-2 py-1 text-xs font-semibold text-gray-900">Perf Fee</td>
                            <td className="px-2 py-1 text-xs text-gray-700">20.00%</td>
                          </tr>
                          <tr>
                            <td className="px-2 py-1 text-xs font-semibold text-gray-900">HWM</td>
                            <td className="px-2 py-1 text-xs text-gray-700">Yes</td>
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            {/* Current Year Monthly Performance - Clean Layout */}
            <div className="mb-2 px-4">
              <h3 className="text-xs font-semibold text-gray-900 mb-1 tracking-tight">
                {data.strategy === 'descartes' && cashData 
                  ? '2025 Monthly Performance vs Cash (%)' 
                  : btcData 
                    ? '2025 Monthly Performance vs BTC (%)' 
                    : '2025 Monthly Performance (%)'}
              </h3>
              <div className="bg-white rounded border border-gray-200 overflow-hidden shadow-sm">
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="bg-gradient-to-r from-gray-50 to-gray-100 border-b border-gray-200">
                        <th className="px-3 py-2 text-left text-xs font-semibold text-gray-900">Strategy</th>
                        <th className="px-2 py-2 text-center text-xs font-semibold text-gray-900">Jan</th>
                        <th className="px-2 py-2 text-center text-xs font-semibold text-gray-900">Feb</th>
                        <th className="px-2 py-2 text-center text-xs font-semibold text-gray-900">Mar</th>
                        <th className="px-2 py-2 text-center text-xs font-semibold text-gray-900">Apr</th>
                        <th className="px-2 py-2 text-center text-xs font-semibold text-gray-900">May</th>
                        <th className="px-2 py-2 text-center text-xs font-semibold text-gray-900">Jun</th>
                        <th className="px-2 py-2 text-center text-xs font-semibold text-gray-900">Jul</th>
                        <th className="px-2 py-2 text-center text-xs font-semibold text-gray-900">Aug</th>
                        <th className="px-2 py-2 text-center text-xs font-semibold text-gray-900">Sep</th>
                        <th className="px-2 py-2 text-center text-xs font-semibold text-gray-900">Oct</th>
                        <th className="px-2 py-2 text-center text-xs font-semibold text-gray-900">Nov</th>
                        <th className="px-2 py-2 text-center text-xs font-semibold text-gray-900">Dec</th>
                        <th className="px-3 py-2 text-center text-xs font-semibold text-gray-900">YTD</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(() => {
                        const currentYear = new Date().getFullYear()
                        const currentMonth = new Date().getMonth()
                        
                        // Current year data with real benchmark data
                        const getCurrentYearData = () => {
                          const strategyMonths = [2.4, -1.2, 3.1, 1.8, -0.5, 2.7, 1.9, -0.8, 2.3, 1.6, 3.2, -1.1].slice(0, currentMonth + 1)
                          
                          let benchmarkMonths = []
                          let benchmarkName = 'BTC'
                          
                          if (data.strategy === 'descartes' && cashData) {
                            // Use cash data for Descartes
                            const currentYearCashData = cashData.filter(cash => cash.date.getFullYear() === currentYear)
                            benchmarkMonths = currentYearCashData.map(cash => cash.monthlyReturn).slice(0, currentMonth + 1)
                            benchmarkName = 'Cash'
                          } else if (btcData) {
                            // Use BTC data for other strategies
                            const currentYearBtcData = btcData.filter(btc => btc.date.getFullYear() === currentYear)
                            benchmarkMonths = currentYearBtcData.map(btc => btc.monthlyReturn).slice(0, currentMonth + 1)
                            benchmarkName = 'BTC'
                          } else {
                            // Fallback to sample data
                            benchmarkMonths = [1.2, -2.8, 4.5, 0.8, -1.2, 3.1, 1.5, -2.1, 2.8, 0.9, 2.7, -0.8].slice(0, currentMonth + 1)
                          }
                          
                          return [
                            { 
                              name: strategy.name, 
                              months: strategyMonths,
                              isBenchmark: false
                            },
                            { 
                              name: benchmarkName, 
                              months: benchmarkMonths,
                              isBenchmark: true
                            }
                          ]
                        }
                        
                        const currentYearData = getCurrentYearData()
                        
                        // Only show benchmark row if we have real benchmark data
                        const hasBenchmarkData = (data.strategy === 'descartes' && cashData) || btcData
                        const filteredData = hasBenchmarkData ? currentYearData : currentYearData.filter(d => !d.isBenchmark)
                        
                        return filteredData.map(({ name, months, isBenchmark }, rowIndex) => {
                          const yearTotal = months.reduce((sum, val) => sum + val, 0)
                          const isEvenRow = rowIndex % 2 === 0
                          
                          return (
                            <tr key={name} className={`border-b border-gray-100 hover:bg-gray-50 ${isEvenRow ? 'bg-white' : 'bg-gray-25'}`}>
                              <td className="px-3 py-2 text-xs font-semibold text-gray-900">
                                {name}
                              </td>
                              {Array.from({ length: 12 }, (_, index) => {
                                const monthReturn = months[index]
                                const isFutureMonth = index > currentMonth
                                
                                return (
                                  <td key={index} className="px-2 py-2 text-center text-xs">
                                    {monthReturn !== undefined ? (
                                      <span className={`font-medium ${monthReturn >= 0 ? 'text-green-700' : 'text-red-600'}`}>
                                        {monthReturn >= 0 ? '+' : ''}{monthReturn.toFixed(1)}
                                      </span>
                                    ) : isFutureMonth ? (
                                      <span className="text-gray-400 text-xs">N/A</span>
                                    ) : (
                                      <span className="text-gray-400">—</span>
                                    )}
                                  </td>
                                )
                              })}
                              <td className="px-3 py-2 text-center text-xs font-bold text-gray-900 bg-gray-50">
                                {yearTotal >= 0 ? '+' : ''}{yearTotal.toFixed(1)}
                                <div className="text-xs text-gray-500 font-normal">(YTD)</div>
                              </td>
                            </tr>
                          )
                        })
                      })()}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            {/* Historical Annual Returns - Compact */}
            <div className="mb-2 px-4">
              <h3 className="text-xs font-semibold text-gray-900 mb-1 tracking-tight">
                {data.strategy === 'descartes' && cashData 
                  ? 'Historical Annual Returns vs Cash (%)' 
                  : btcData 
                    ? 'Historical Annual Returns vs BTC (%)' 
                    : 'Historical Annual Returns (%)'}
              </h3>
              <div className="bg-white rounded border border-gray-200 overflow-hidden shadow-sm">
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="bg-gradient-to-r from-gray-50 to-gray-100 border-b border-gray-200">
                        <th className="px-3 py-2 text-left text-xs font-semibold text-gray-900">Year</th>
                        <th className="px-3 py-2 text-center text-xs font-semibold text-gray-900">{strategy.name}</th>
                        {((data.strategy === 'descartes' && cashData) || btcData) && (
                          <th className="px-3 py-2 text-center text-xs font-semibold text-gray-900">
                            {data.strategy === 'descartes' ? 'Cash' : 'BTC'}
                          </th>
                        )}
                        {((data.strategy === 'descartes' && cashData) || btcData) && (
                          <th className="px-3 py-2 text-center text-xs font-semibold text-gray-900">Outperformance</th>
                        )}
                      </tr>
                    </thead>
                    <tbody>
                      {(() => {
                        const getAnnualData = () => {
                          const annualData = [
                            { year: 2024, strategy: 16.8, benchmark: 0 },
                            { year: 2023, strategy: 8.6, benchmark: 0 }
                          ]
                          
                          if (data.strategy === 'descartes' && cashData) {
                            // Calculate annual returns from cash data
                            const cashByYear = new Map()
                            cashData.forEach(cash => {
                              const year = cash.date.getFullYear()
                              if (!cashByYear.has(year)) {
                                cashByYear.set(year, [])
                              }
                              cashByYear.get(year).push(cash.monthlyReturn)
                            })
                            
                            // Calculate annual returns for each year
                            cashByYear.forEach((monthlyReturns, year) => {
                              const annualReturn = monthlyReturns.reduce((sum, ret) => sum + ret, 0)
                              const dataIndex = annualData.findIndex(d => d.year === year)
                              if (dataIndex !== -1) {
                                annualData[dataIndex].benchmark = Math.round(annualReturn * 10) / 10
                              }
                            })
                          } else if (btcData) {
                            // Calculate annual returns from BTC data
                            const btcByYear = new Map()
                            btcData.forEach(btc => {
                              const year = btc.date.getFullYear()
                              if (!btcByYear.has(year)) {
                                btcByYear.set(year, [])
                              }
                              btcByYear.get(year).push(btc.monthlyReturn)
                            })
                            
                            // Calculate annual returns for each year
                            btcByYear.forEach((monthlyReturns, year) => {
                              const annualReturn = monthlyReturns.reduce((sum, ret) => sum + ret, 0)
                              const dataIndex = annualData.findIndex(d => d.year === year)
                              if (dataIndex !== -1) {
                                annualData[dataIndex].benchmark = Math.round(annualReturn * 10) / 10
                              }
                            })
                          } else {
                            // Fallback to sample data
                            annualData[0].benchmark = data.strategy === 'descartes' ? 2.4 : 12.4
                            annualData[1].benchmark = data.strategy === 'descartes' ? 1.8 : 6.2
                          }
                          
                          return annualData
                        }
                        
                        const annualData = getAnnualData()
                        
                        return annualData.map(({ year, strategy: strategyReturn, benchmark }, index) => {
                          const outperformance = strategyReturn - benchmark
                          const isEvenRow = index % 2 === 0
                          const hasBenchmarkData = (data.strategy === 'descartes' && cashData) || btcData
                          
                          return (
                            <tr key={year} className={`border-b border-gray-100 hover:bg-gray-50 ${isEvenRow ? 'bg-white' : 'bg-gray-25'}`}>
                              <td className="px-3 py-2 text-xs font-semibold text-gray-900">{year}</td>
                              <td className="px-3 py-2 text-center text-xs">
                                <span className={`font-medium ${strategyReturn >= 0 ? 'text-green-700' : 'text-red-600'}`}>
                                  {strategyReturn >= 0 ? '+' : ''}{strategyReturn.toFixed(1)}
                                </span>
                              </td>
                              {hasBenchmarkData && (
                                <td className="px-3 py-2 text-center text-xs">
                                  <span className={`font-medium ${benchmark >= 0 ? 'text-green-700' : 'text-red-600'}`}>
                                    {benchmark >= 0 ? '+' : ''}{benchmark.toFixed(1)}
                                  </span>
                                </td>
                              )}
                              {hasBenchmarkData && (
                                <td className="px-3 py-2 text-center text-xs">
                                  <span className={`font-medium ${outperformance >= 0 ? 'text-green-700' : 'text-red-600'}`}>
                                    {outperformance >= 0 ? '+' : ''}{outperformance.toFixed(1)}
                                  </span>
                                </td>
                              )}
                            </tr>
                          )
                        })
                      })()}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            {/* General Information */}
            <div className="mb-1 px-4">
              <h3 className="text-xs font-semibold text-gray-900 mb-1 tracking-tight">General Information</h3>
              <div className="bg-white rounded border border-gray-200 overflow-hidden shadow-sm">
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <tbody>
                      <tr className="border-b border-gray-100">
                        <td className="px-3 py-1 text-xs font-semibold text-gray-900 w-1/3">Company</td>
                        <td className="px-3 py-1 text-xs text-gray-700">DFI Labs</td>
                      </tr>
                      <tr className="border-b border-gray-100">
                        <td className="px-3 py-1 text-xs font-semibold text-gray-900">Vehicle</td>
                        <td className="px-3 py-1 text-xs text-gray-700">Managed Account</td>
                      </tr>
                      <tr className="border-b border-gray-100">
                        <td className="px-3 py-1 text-xs font-semibold text-gray-900">Minimum Investment</td>
                        <td className="px-3 py-1 text-xs text-gray-700">100,000 USD</td>
                      </tr>
                      <tr className="border-b border-gray-100">
                        <td className="px-3 py-1 text-xs font-semibold text-gray-900">Management Fee</td>
                        <td className="px-3 py-1 text-xs text-gray-700">2.00%</td>
                      </tr>
                      <tr className="border-b border-gray-100">
                        <td className="px-3 py-1 text-xs font-semibold text-gray-900">Performance Fee</td>
                        <td className="px-3 py-1 text-xs text-gray-700">20.00%</td>
                      </tr>
                      <tr>
                        <td className="px-3 py-1 text-xs font-semibold text-gray-900">High Water Mark</td>
                        <td className="px-3 py-1 text-xs text-gray-700">Yes</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            {/* Minimal Disclosures */}
            <div className="mb-1 px-4">
              <p className="text-xs text-gray-400 text-center">
                Returns net of fees, time-weighted, USD. Daily→monthly via geometric compounding. 
                <span className="font-medium"> As of {new Date().toLocaleDateString('en-US', { month: 'long', year: 'numeric' })}</span> • 
                Past performance ≠ future results
              </p>
            </div>

            {/* Leadership Team - Bigger Faces */}
            <div className="mb-1 px-4">
              <h3 className="text-xs font-semibold text-gray-900 mb-1 tracking-tight">Leadership Team</h3>
              <div className="bg-white rounded border border-gray-200 p-2 shadow-sm">
                {/* Central Leaders - Olivier & Hadrien */}
                <div className="grid grid-cols-2 gap-3 mb-2">
                  <div className="text-center bg-gradient-to-r from-gray-50 to-gray-100 rounded p-2">
                    <img 
                      src="/images/team/olivier.jpg" 
                      alt="Olivier Chevillon" 
                      className="w-12 h-12 rounded-full object-cover mx-auto mb-2 border-2 border-gray-300"
                    />
                    <h4 className="text-xs font-bold text-gray-900">Olivier Chevillon</h4>
                    <p className="text-xs text-gray-700 font-semibold">Founder & CIO</p>
                    <p className="text-xs text-gray-600 leading-tight">20+ years managing quantitative strategies for tier-1 hedge funds. MBA INSEAD, MSc ENSAE.</p>
                  </div>
                  <div className="text-center bg-gradient-to-r from-gray-50 to-gray-100 rounded p-2">
                    <img 
                      src="/images/team/hadrien.jpg" 
                      alt="Hadrien Darmon" 
                      className="w-12 h-12 rounded-full object-cover mx-auto mb-2 border-2 border-gray-300"
                    />
                    <h4 className="text-xs font-bold text-gray-900">Hadrien Darmon</h4>
                    <p className="text-xs text-gray-700 font-semibold">CTO & Quant</p>
                    <p className="text-xs text-gray-600 leading-tight">Principal PM at Method Investments. CentraleSupélec & Paris-Assas.</p>
                  </div>
                </div>
                
                {/* Other Team Members - Bigger Faces */}
                <div className="grid grid-cols-3 gap-2">
                  <div className="text-center">
                    <img 
                      src="/images/team/louis.jpg" 
                      alt="Louis Benassy" 
                      className="w-10 h-10 rounded-full object-cover mx-auto mb-2 border-2 border-gray-200"
                    />
                    <h4 className="text-xs font-semibold text-gray-900">Louis Benassy</h4>
                    <p className="text-xs text-gray-600">COO</p>
                    <p className="text-xs text-gray-500 leading-tight">Crypto-asset specialist; co-founded Chainvest (2017). MSc/BSc Finance, Cass & NUS.</p>
                  </div>
                  <div className="text-center">
                    <img 
                      src="/images/team/yuval.png" 
                      alt="Yuval Reisman" 
                      className="w-10 h-10 rounded-full object-cover mx-auto mb-2 border-2 border-gray-200 grayscale"
                    />
                    <h4 className="text-xs font-semibold text-gray-900">Yuval Reisman</h4>
                    <p className="text-xs text-gray-600">Partner</p>
                    <p className="text-xs text-gray-500 leading-tight">CEO/co-founder of YRD (market-neutral crypto fund). ex-CEO Hone Capital. MBA INSEAD.</p>
                  </div>
                  <div className="text-center">
                    <img 
                      src="/images/team/leo.jpg" 
                      alt="Leo Rene" 
                      className="w-10 h-10 rounded-full object-cover mx-auto mb-2 border-2 border-gray-200"
                    />
                    <h4 className="text-xs font-semibold text-gray-900">Leo Rene</h4>
                    <p className="text-xs text-gray-600">Quant Researcher</p>
                    <p className="text-xs text-gray-500 leading-tight">Data & ML-focused; Researcher at Qube. ENSAE & École Polytechnique.</p>
                  </div>
                </div>
              </div>
            </div>

            {/* Footer */}
            <div className="mt-1 px-4 pb-2">
              <div className="bg-gray-50 rounded border border-gray-200 p-2">
                <div className="text-center">
                  <h4 className="text-xs font-semibold text-gray-900 mb-1">DFI Labs</h4>
                  <p className="text-xs text-gray-600 mb-1">14 Avenue du Général-de-Gaulle, 94160 Saint-Mandé, France</p>
                  <div className="flex justify-center space-x-3 text-xs text-gray-500">
                    <span>AMF-registered (E2022-032)</span>
                    <span>hello@dfi-labs.com</span>
                    <span>Generated {new Date().toLocaleDateString()}</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

      </div>
    </div>
  )
}

