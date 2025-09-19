# 🚀 Quick Deployment to admin.factsheet.dfi-labs.com

## ✅ Ready to Deploy!

Your DFI Labs Factsheet Platform is now ready for production deployment.

## 🎯 Quick Start (3 Steps)

### 1. Upload to Server
```bash
# Upload the entire project folder to your server
scp -r /Users/dfilabs/dfi-factsheet-platform user@your-server:/var/www/
```

### 2. SSH and Start
```bash
ssh user@your-server
cd /var/www/dfi-factsheet-platform
chmod +x start-production.sh
./start-production.sh
```

### 3. Configure Domain
Point `admin.factsheet.dfi-labs.com` to your server IP with SSL.

## 🔧 What's Included

- ✅ **Production Build**: Optimized and ready
- ✅ **Real BTC Data**: Live CoinGecko API integration
- ✅ **Security Headers**: X-Frame-Options, CORS, etc.
- ✅ **Health Check**: `/api/health` endpoint
- ✅ **Docker Support**: Dockerfile + docker-compose.yml
- ✅ **Process Management**: PM2 integration
- ✅ **SSL Ready**: HTTPS configuration

## 📊 Features Live

- 🎨 **Beautiful PDF Generation**: Apple-style elegant factsheets
- 📈 **Real Performance Data**: Actual CSV data processing
- 🏆 **BTC Benchmark**: Live Bitcoin performance comparison
- 👥 **Team Profiles**: Professional team presentation
- 📋 **Fund Details**: Management fees, minimums, etc.
- 🔒 **Admin Authentication**: Secure access control

## 🌐 Access URLs

- **Main Platform**: https://admin.factsheet.dfi-labs.com
- **Health Check**: https://admin.factsheet.dfi-labs.com/api/health
- **Admin Login**: https://admin.factsheet.dfi-labs.com/auth/login

## 🚨 Important Notes

1. **Domain Configuration**: Ensure DNS points to your server
2. **SSL Certificate**: Required for HTTPS
3. **Port 3000**: Application runs on this port
4. **Reverse Proxy**: Configure Nginx/Apache to proxy to port 3000

## 📞 Support

If you encounter issues:
1. Check server logs
2. Verify domain DNS settings
3. Ensure SSL certificate is valid
4. Confirm port 3000 is accessible

---

**🎉 Your professional factsheet platform is ready to go live!**
