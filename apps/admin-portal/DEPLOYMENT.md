# DFI Labs Factsheet Platform - Deployment Guide

## 🚀 Deploying to admin.factsheet.dfi-labs.com

This guide covers multiple deployment options for the DFI Labs Factsheet Platform.

## 📋 Prerequisites

- Node.js 18+ installed
- Domain `admin.factsheet.dfi-labs.com` configured
- Server with Docker (optional)
- SSL certificate for HTTPS

## 🎯 Deployment Options

### Option 1: Direct Server Deployment (Recommended)

1. **Upload files to your server:**
   ```bash
   # Upload the entire project folder to your server
   scp -r /Users/dfilabs/dfi-factsheet-platform user@your-server:/var/www/
   ```

2. **SSH into your server:**
   ```bash
   ssh user@your-server
   cd /var/www/dfi-factsheet-platform
   ```

3. **Run the deployment script:**
   ```bash
   chmod +x deploy.sh
   ./deploy.sh
   ```

4. **Configure reverse proxy (Nginx):**
   ```nginx
   server {
       listen 80;
       server_name admin.factsheet.dfi-labs.com;
       return 301 https://$server_name$request_uri;
   }

   server {
       listen 443 ssl http2;
       server_name admin.factsheet.dfi-labs.com;

       ssl_certificate /path/to/your/certificate.crt;
       ssl_certificate_key /path/to/your/private.key;

       location / {
           proxy_pass http://localhost:3000;
           proxy_http_version 1.1;
           proxy_set_header Upgrade $http_upgrade;
           proxy_set_header Connection 'upgrade';
           proxy_set_header Host $host;
           proxy_set_header X-Real-IP $remote_addr;
           proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
           proxy_set_header X-Forwarded-Proto $scheme;
           proxy_cache_bypass $http_upgrade;
       }
   }
   ```

### Option 2: Docker Deployment

1. **Build and run with Docker Compose:**
   ```bash
   docker-compose up -d --build
   ```

2. **Configure reverse proxy to point to port 3000**

### Option 3: Vercel Deployment (Easiest)

1. **Install Vercel CLI:**
   ```bash
   npm i -g vercel
   ```

2. **Deploy:**
   ```bash
   vercel --prod
   ```

3. **Configure custom domain in Vercel dashboard**

## 🔧 Environment Configuration

Create a `.env.local` file on your server:

```env
NODE_ENV=production
NEXT_PUBLIC_APP_URL=https://admin.factsheet.dfi-labs.com
```

## 🔒 Security Considerations

- ✅ SSL certificate configured
- ✅ Security headers enabled (X-Frame-Options, etc.)
- ✅ Admin authentication required
- ✅ CORS properly configured

## 📊 Monitoring

- Application runs on port 3000
- Health check endpoint: `https://admin.factsheet.dfi-labs.com/api/health`
- Logs available in server console

## 🚨 Troubleshooting

### Common Issues:

1. **Domain not resolving:**
   - Check DNS configuration
   - Ensure A record points to server IP

2. **SSL certificate issues:**
   - Verify certificate is valid
   - Check certificate chain

3. **Application not starting:**
   - Check Node.js version (18+)
   - Verify all dependencies installed
   - Check port 3000 is available

4. **PDF generation issues:**
   - Ensure CoinGecko API is accessible
   - Check network connectivity

## 📞 Support

For deployment issues, check:
- Server logs: `pm2 logs` or `docker logs`
- Application logs in browser console
- Network connectivity to external APIs

## 🔄 Updates

To update the application:

1. Pull latest changes
2. Run `npm install`
3. Run `npm run build`
4. Restart the application

---

**🎉 Your DFI Labs Factsheet Platform will be live at: https://admin.factsheet.dfi-labs.com**
