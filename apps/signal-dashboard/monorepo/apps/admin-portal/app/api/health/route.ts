import { NextResponse } from 'next/server'

export async function GET() {
  return NextResponse.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    service: 'DFI Labs Factsheet Platform',
    version: '1.0.0'
  })
}
