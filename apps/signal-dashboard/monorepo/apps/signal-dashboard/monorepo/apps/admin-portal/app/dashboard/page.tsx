'use client'

import { useState, useEffect } from 'react'
import { useRouter } from 'next/navigation'
import { motion } from 'framer-motion'
import { 
  FileText, 
  Upload, 
  Database, 
  Settings, 
  Users, 
  BarChart3, 
  LogOut,
  Plus,
  Eye,
  Download,
  Trash2,
  Edit
} from 'lucide-react'

interface Factsheet {
  id: string
  title: string
  type: 'csv' | 'live'
  status: 'draft' | 'published' | 'archived'
  createdAt: string
  updatedAt: string
  views: number
}

export default function DashboardPage() {
  const [isAuthenticated, setIsAuthenticated] = useState(false)
  const [isLoading, setIsLoading] = useState(true)
  const [activeTab, setActiveTab] = useState<'overview' | 'factsheets' | 'settings'>('overview')
  const [factsheets, setFactsheets] = useState<Factsheet[]>([
    {
      id: '1',
      title: 'Q4 2024 Portfolio Performance',
      type: 'live',
      status: 'published',
      createdAt: '2024-01-15',
      updatedAt: '2024-01-20',
      views: 1247
    },
    {
      id: '2',
      title: 'Risk Analysis Report',
      type: 'csv',
      status: 'draft',
      createdAt: '2024-01-18',
      updatedAt: '2024-01-19',
      views: 0
    },
    {
      id: '3',
      title: 'Market Overview Q1 2024',
      type: 'live',
      status: 'published',
      createdAt: '2024-01-10',
      updatedAt: '2024-01-15',
      views: 892
    }
  ])
  const router = useRouter()

  useEffect(() => {
    // Check authentication
    const auth = localStorage.getItem('adminAuth')
    if (auth === 'true') {
      setIsAuthenticated(true)
    } else {
      router.push('/auth/login')
    }
    setIsLoading(false)
  }, [router])

  const handleLogout = () => {
    localStorage.removeItem('adminAuth')
    localStorage.removeItem('adminUser')
    router.push('/auth/login')
  }

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'published': return 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
      case 'draft': return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400'
      case 'archived': return 'bg-gray-100 text-gray-800 dark:bg-gray-900/20 dark:text-gray-400'
      default: return 'bg-gray-100 text-gray-800 dark:bg-gray-900/20 dark:text-gray-400'
    }
  }

  if (isLoading) {
    return (
      <div className="min-h-screen dfi-gradient flex items-center justify-center">
        <div className="w-8 h-8 border-2 border-dfi-accent border-t-transparent rounded-full animate-spin" />
      </div>
    )
  }

  if (!isAuthenticated) {
    return null
  }

  return (
    <div className="min-h-screen dfi-gradient">
      {/* Header */}
      <header className="border-b border-dfi-border bg-white/80 backdrop-blur-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-6">
            <div className="flex items-center space-x-3">
              <div className="w-10 h-10 dfi-accent-gradient rounded-lg flex items-center justify-center">
                <span className="text-white font-bold text-xl">DF</span>
              </div>
              <div>
                <h1 className="text-2xl font-bold dfi-text-gradient">DFI Labs Admin</h1>
                <p className="text-dfi-text-light text-sm">Factsheet Platform Management</p>
              </div>
            </div>
            <div className="flex items-center space-x-4">
              <button
                onClick={handleLogout}
                className="flex items-center space-x-2 text-dfi-text-light hover:text-dfi-text transition-colors"
              >
                <LogOut className="w-5 h-5" />
                <span>Logout</span>
              </button>
            </div>
          </div>
        </div>
      </header>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Navigation Tabs */}
        <div className="flex space-x-1 mb-8">
          {[
            { id: 'overview', label: 'Overview', icon: BarChart3 },
            { id: 'factsheets', label: 'Factsheets', icon: FileText },
            { id: 'settings', label: 'Settings', icon: Settings }
          ].map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              onClick={() => setActiveTab(id as any)}
              className={`flex items-center space-x-2 px-4 py-2 rounded-lg transition-all ${
                activeTab === id
                  ? 'dfi-accent-gradient text-white shadow-lg'
                  : 'text-dfi-text-light hover:text-dfi-text hover:bg-white/50'
              }`}
            >
              <Icon className="w-4 h-4" />
              <span>{label}</span>
            </button>
          ))}
        </div>

        {/* Tab Content */}
        {activeTab === 'overview' && (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="space-y-8"
          >
            {/* Stats Cards */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
              {[
                { title: 'Total Factsheets', value: '12', icon: FileText, color: 'text-blue-500' },
                { title: 'Published', value: '8', icon: Eye, color: 'text-green-500' },
                { title: 'Drafts', value: '3', icon: Edit, color: 'text-yellow-500' },
                { title: 'Total Views', value: '2,847', icon: BarChart3, color: 'text-purple-500' }
              ].map((stat, index) => (
                <motion.div
                  key={stat.title}
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: index * 0.1 }}
                  className="dfi-card p-6"
                >
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm text-dfi-gray">{stat.title}</p>
                      <p className="text-2xl font-bold text-white">{stat.value}</p>
                    </div>
                    <stat.icon className={`w-8 h-8 ${stat.color}`} />
                  </div>
                </motion.div>
              ))}
            </div>

            {/* Recent Activity */}
            <div className="dfi-card p-6">
              <h3 className="text-lg font-semibold text-white mb-4">Recent Activity</h3>
              <div className="space-y-4">
                {[
                  { action: 'Published', title: 'Q4 2024 Portfolio Performance', time: '2 hours ago' },
                  { action: 'Updated', title: 'Risk Analysis Report', time: '1 day ago' },
                  { action: 'Created', title: 'Market Overview Q1 2024', time: '3 days ago' }
                ].map((activity, index) => (
                  <div key={index} className="flex items-center justify-between py-2 border-b border-dfi-border/20 last:border-b-0">
                    <div>
                      <span className="text-dfi-accent font-medium">{activity.action}</span>
                      <span className="text-white ml-2">{activity.title}</span>
                    </div>
                    <span className="text-dfi-gray text-sm">{activity.time}</span>
                  </div>
                ))}
              </div>
            </div>
          </motion.div>
        )}

        {activeTab === 'factsheets' && (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="space-y-6"
          >
            {/* Actions Bar */}
            <div className="flex justify-between items-center">
              <h2 className="text-xl font-semibold text-white">Factsheets</h2>
              <button className="dfi-button-primary flex items-center space-x-2">
                <Plus className="w-4 h-4" />
                <span>New Factsheet</span>
              </button>
            </div>

            {/* Factsheets Table */}
            <div className="dfi-card overflow-hidden">
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead className="bg-dfi-secondary/50">
                    <tr>
                      <th className="px-6 py-3 text-left text-xs font-medium text-dfi-gray uppercase tracking-wider">Title</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-dfi-gray uppercase tracking-wider">Type</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-dfi-gray uppercase tracking-wider">Status</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-dfi-gray uppercase tracking-wider">Views</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-dfi-gray uppercase tracking-wider">Updated</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-dfi-gray uppercase tracking-wider">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-dfi-border/20">
                    {factsheets.map((factsheet) => (
                      <tr key={factsheet.id} className="hover:bg-dfi-secondary/20">
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="text-sm font-medium text-white">{factsheet.title}</div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${
                            factsheet.type === 'live' 
                              ? 'bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-400'
                              : 'bg-gray-100 text-gray-800 dark:bg-gray-900/20 dark:text-gray-400'
                          }`}>
                            {factsheet.type.toUpperCase()}
                          </span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${getStatusColor(factsheet.status)}`}>
                            {factsheet.status}
                          </span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-dfi-gray">
                          {factsheet.views.toLocaleString()}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-dfi-gray">
                          {new Date(factsheet.updatedAt).toLocaleDateString()}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm font-medium">
                          <div className="flex space-x-2">
                            <button className="text-dfi-accent hover:text-dfi-accent/80">
                              <Eye className="w-4 h-4" />
                            </button>
                            <button className="text-dfi-accent hover:text-dfi-accent/80">
                              <Edit className="w-4 h-4" />
                            </button>
                            <button className="text-dfi-accent hover:text-dfi-accent/80">
                              <Download className="w-4 h-4" />
                            </button>
                            <button className="text-red-400 hover:text-red-300">
                              <Trash2 className="w-4 h-4" />
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </motion.div>
        )}

        {activeTab === 'settings' && (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="space-y-6"
          >
            <h2 className="text-xl font-semibold text-white">Settings</h2>
            <div className="dfi-card p-6">
              <h3 className="text-lg font-medium text-white mb-4">Platform Configuration</h3>
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-dfi-gray mb-2">Platform Name</label>
                  <input
                    type="text"
                    defaultValue="DFI Labs Factsheet Platform"
                    className="dfi-input"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-dfi-gray mb-2">Admin Email</label>
                  <input
                    type="email"
                    defaultValue="admin@dfi-labs.com"
                    className="dfi-input"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-dfi-gray mb-2">Default Timezone</label>
                  <select className="dfi-input">
                    <option>UTC</option>
                    <option>EST</option>
                    <option>PST</option>
                  </select>
                </div>
                <button className="dfi-button-primary">Save Settings</button>
              </div>
            </div>
          </motion.div>
        )}
      </div>
    </div>
  )
}
