# Database Service

A high-performance REST API service for V3 arc detection database operations with connection pooling, comprehensive error handling, and rate limiting.

## 🚀 Quick Start

```bash
# Install dependencies
pip install flask flask-limiter numpy sqlite3

# Start the service
python app.py

# Service will be available at http://localhost:5001
```

## 📋 Features

- **REST API**: Complete CRUD operations for arc detection data
- **Connection Pooling**: Thread-safe SQLite connection pooling for better performance
- **Error Handling**: Comprehensive error responses with proper HTTP status codes
- **Rate Limiting**: Configurable rate limiting to prevent abuse
- **Logging**: Detailed logging for debugging and monitoring
- **Health Checks**: Built-in health check endpoint

## 🔗 API Endpoints

### Health & Info
- `GET /health` - Health check
- `GET /` - Service information

### File Operations
- `GET /api/files` - List all files (with optional label filter)
- `GET /api/files/{id}` - Get file details
- `GET /api/files/{id}/data` - Get signal data
- `PUT /api/files/{id}/label` - Update file label
- `PUT /api/files/{id}/transients` - Update transient indices

### Search & Statistics
- `GET /api/search` - Search files by criteria
- `GET /api/labels/statistics` - Label statistics
- `GET /api/status` - Status summary

## 💻 Usage Examples

```bash
# Get all arc files
curl "http://localhost:5001/api/files?label=arc"

# Get file 1 details
curl http://localhost:5001/api/files/1

# Update file label
curl -X PUT http://localhost:5001/api/files/1/label \
  -H "Content-Type: application/json" \
  -d '{"label": "weak_arc"}'

# Search files
curl "http://localhost:5001/api/search?voltage_min=200&voltage_max=300"
```

## ⚙️ Configuration

### Database Configuration
Update in `database/operations.py`:
```python
DATABASE_PATH = "/path/to/your/arc_detection.db"
BINARY_DATA_DIR = "/path/to/your/fileset"
```

### Rate Limiting
Update in `app.py`:
```python
default_limits=["1000 per hour", "100 per minute"]
```

### Connection Pool
Update in `database/operations.py`:
```python
DatabaseConnectionPool(db_path, pool_size=10, timeout=30)
```

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Flask App     │───▶│  Connection     │───▶│  SQLite         │
│   (app.py)      │    │  Pool           │    │  Database       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                                               
         ▼                                               
┌─────────────────┐                                      
│   API Routes    │                                      
│   (api/files.py)│                                      
└─────────────────┘                                      
```

## 🔧 Development

### Project Structure
```
database/
├── app.py                 # Main Flask application
├── api/
│   └── files.py          # API route handlers
├── database/
│   └── operations.py     # Database operations with connection pooling
└── tools/
    └── init_db.py        # Database initialization
```

### Adding New Endpoints
1. Add route handler in `api/files.py`
2. Use `@handle_errors` decorator
3. Implement validation with helper functions
4. Update this README with new endpoint info

### Error Handling
All endpoints use comprehensive error handling:
- **400**: Validation errors
- **404**: Resource not found
- **500**: Database or internal errors

### Logging
Configure logging level in `api/files.py`:
```python
logging.basicConfig(level=logging.INFO)
```

## 🧪 Testing

```bash
# Test health check
curl http://localhost:5001/health

# Test API endpoints
curl http://localhost:5001/api/files

# Test error handling
curl http://localhost:5001/api/files/999999
```

## 🐛 Troubleshooting

### Database Connection Issues
- Check `DATABASE_PATH` configuration
- Ensure database file exists and is readable
- Verify SQLite version compatibility

### Performance Issues
- Increase connection pool size
- Check for database locks
- Monitor connection pool utilization

### Rate Limiting
- Adjust limits in `app.py`
- Check client IP for rate limit status
- Monitor rate limit logs

## 📊 Monitoring

### Health Check Response
```json
{
  "status": "healthy",
  "service": "database",
  "version": "20250602_100100_0_0_1_1"
}
```

### Error Response Format
```json
{
  "status": "error",
  "error_type": "validation_error",
  "message": "File ID must be a positive integer"
}
```

## 🚢 Deployment

### Docker (Optional)
```dockerfile
FROM python:3.9-slim
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
EXPOSE 5001
CMD ["python", "app.py"]
```

### Production Deployment
- Use a production WSGI server (gunicorn, uwsgi)
- Configure reverse proxy (nginx)
- Set up monitoring and logging
- Configure database backups

## 🔗 Integration

This service is designed to work with:
- **Annotation Service** (port 5002) - Consumes this API
- **Styles Gallery** (port 5003) - May use for metadata
- **Main Application** - Uses this as data layer

## 📝 Version History

- **20250602_101700_0_0_1_5**: Added comprehensive error handling and validation
- **20250602_100100_0_0_1_1**: Initial release with basic REST API

## 🤝 Contributing

1. Follow the existing code style
2. Add comprehensive error handling
3. Update API documentation
4. Add tests for new features
5. Update version numbers appropriately

## 📄 License

Part of the data processor project - see main repository for license details.