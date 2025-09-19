'use client'

import { useState } from 'react'
import { motion } from 'framer-motion'
import { 
  FileText, 
  BarChart3, 
  Settings, 
  Users, 
  Database,
  ExternalLink,
  ArrowRight
} from 'lucide-react'

export default function AdminPanel() {
  const [hoveredModule, setHoveredModule] = useState<string | null>(null)

  const modules = [
    {
      id: 'factsheet',
      title: 'Factsheet Generator',
      description: 'Generate professional factsheets from CSV data or live accounts',
      icon: FileText,
      color: 'from-blue-500 to-blue-600',
      url: 'https://admin.dfi-labs.com/factsheet/',
      status: 'active'
    },
    {
      id: 'signal-dashboard',
      title: 'Signal Dashboard',
      description: 'Monitor trading signals and strategy performance in real-time',
      icon: BarChart3,
      color: 'from-green-500 to-green-600',
      url: 'https://admin.dfi-labs.com/signal-dashboard/',
      status: 'active'
    },
    {
      id: 'user-management',
      title: 'User Management',
      description: 'Manage user accounts, permissions, and access controls',
      icon: Users,
      color: 'from-purple-500 to-purple-600',
      url: '#',
      status: 'coming-soon'
    },
    {
      id: 'data-management',
      title: 'Data Management',
      description: 'Upload, process, and manage historical trading data',
      icon: Database,
      color: 'from-orange-500 to-orange-600',
      url: '#',
      status: 'coming-soon'
    },
    {
      id: 'settings',
      title: 'System Settings',
      description: 'Configure platform settings, integrations, and preferences',
      icon: Settings,
      color: 'from-gray-500 to-gray-600',
      url: '#',
      status: 'coming-soon'
    }
  ]

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'active':
        return <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800">Active</span>
      case 'coming-soon':
        return <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">Coming Soon</span>
      default:
        return null
    }
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-6">
            <div className="flex items-center space-x-3">
              <div className="w-12 h-12 bg-gradient-to-r from-indigo-600 to-purple-600 rounded-xl flex items-center justify-center">
                <span className="text-white font-bold text-xl">DF</span>
              </div>
              <div>
                <h1 className="text-3xl font-bold bg-gradient-to-r from-indigo-600 to-purple-600 bg-clip-text text-transparent">
                  DFI Labs Admin
                </h1>
                <p className="text-gray-600 text-sm">Investment Management Platform</p>
              </div>
            </div>
            <div className="flex items-center space-x-4">
              <div className="text-right">
                <p className="text-sm font-medium text-gray-900">Admin User</p>
                <p className="text-xs text-gray-500">admin@dfi-labs.com</p>
              </div>
              <div className="w-8 h-8 bg-gradient-to-r from-indigo-600 to-purple-600 rounded-full flex items-center justify-center">
                <span className="text-white text-sm font-medium">A</span>
              </div>
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        {/* Welcome Section */}
        <div className="text-center mb-12">
          <h2 className="text-4xl font-bold text-gray-900 mb-4">
            Welcome to DFI Labs Admin
          </h2>
          <p className="text-xl text-gray-600 max-w-3xl mx-auto">
            Access and manage all your investment management tools and modules from this central dashboard.
          </p>
        </div>

        {/* Modules Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
          {modules.map((module, index) => {
            const Icon = module.icon
            return (
              <motion.div
                key={module.id}
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.1 }}
                className="relative group"
                onMouseEnter={() => setHoveredModule(module.id)}
                onMouseLeave={() => setHoveredModule(null)}
              >
                <div className="bg-white rounded-2xl shadow-lg hover:shadow-xl transition-all duration-300 p-8 h-full border border-gray-200 hover:border-gray-300">
                  {/* Module Header */}
                  <div className="flex items-start justify-between mb-6">
                    <div className={`w-16 h-16 bg-gradient-to-r ${module.color} rounded-xl flex items-center justify-center shadow-lg`}>
                      <Icon className="w-8 h-8 text-white" />
                    </div>
                    {getStatusBadge(module.status)}
                  </div>

                  {/* Module Content */}
                  <div className="mb-6">
                    <h3 className="text-xl font-semibold text-gray-900 mb-3">
                      {module.title}
                    </h3>
                    <p className="text-gray-600 leading-relaxed">
                      {module.description}
                    </p>
                  </div>

                  {/* Module Footer */}
                  <div className="flex items-center justify-between">
                    {module.status === 'active' ? (
                      <a
                        href={module.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="inline-flex items-center space-x-2 text-indigo-600 hover:text-indigo-700 font-medium transition-colors"
                      >
                        <span>Open Module</span>
                        <ExternalLink className="w-4 h-4" />
                      </a>
                    ) : (
                      <span className="text-gray-400 font-medium">
                        Coming Soon
                      </span>
                    )}
                    
                    {module.status === 'active' && (
                      <ArrowRight className="w-5 h-5 text-gray-400 group-hover:text-indigo-600 transition-colors" />
                    )}
                  </div>

                  {/* Hover Effect */}
                  {hoveredModule === module.id && module.status === 'active' && (
                    <motion.div
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      className="absolute inset-0 bg-gradient-to-r from-indigo-50 to-purple-50 rounded-2xl border-2 border-indigo-200 pointer-events-none"
                    />
                  )}
                </div>
              </motion.div>
            )
          })}
        </div>

        {/* Quick Stats */}
        <div className="mt-16 bg-white rounded-2xl shadow-lg p-8">
          <h3 className="text-2xl font-bold text-gray-900 mb-8">Platform Overview</h3>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
            <div className="text-center">
              <div className="text-3xl font-bold text-indigo-600 mb-2">2</div>
              <div className="text-gray-600">Active Modules</div>
            </div>
            <div className="text-center">
              <div className="text-3xl font-bold text-green-600 mb-2">24/7</div>
              <div className="text-gray-600">Uptime</div>
            </div>
            <div className="text-center">
              <div className="text-3xl font-bold text-purple-600 mb-2">100%</div>
              <div className="text-gray-600">SSL Secure</div>
            </div>
            <div className="text-center">
              <div className="text-3xl font-bold text-orange-600 mb-2">3</div>
              <div className="text-gray-600">Coming Soon</div>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="mt-16 text-center text-gray-500 text-sm">
          <p>© 2024 DFI Labs. All rights reserved.</p>
          <p className="mt-2">Investment Management Platform v1.0</p>
        </div>
      </main>
    </div>
  )
}