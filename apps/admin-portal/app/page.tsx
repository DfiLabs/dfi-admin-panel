'use client'

import { useEffect } from 'react'
import { useRouter } from 'next/navigation'

export default function HomePage() {
  const router = useRouter()

  useEffect(() => {
    // Redirect to admin panel
    router.push('/admin/')
  }, [router])

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100 flex items-center justify-center">
      <div className="text-center">
        <div className="w-16 h-16 bg-gradient-to-r from-indigo-600 to-purple-600 rounded-xl flex items-center justify-center mx-auto mb-4">
          <span className="text-white font-bold text-2xl">DF</span>
        </div>
        <h1 className="text-2xl font-bold text-gray-900 mb-2">DFI Labs Admin</h1>
        <p className="text-gray-600">Redirecting to admin panel...</p>
      </div>
    </div>
  )
}