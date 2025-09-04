# API Documentation

This directory contains API documentation and specifications for the BarAlgae data infrastructure.

## API Overview

The BarAlgae Data Infrastructure provides RESTful APIs for accessing processed data, analytics, and system status.

## Endpoints

### Data Access APIs

#### FlowCam Data
- `GET /api/v1/flowcam/data` - Retrieve FlowCam data
- `GET /api/v1/flowcam/aggregates` - Get aggregated metrics
- `GET /api/v1/flowcam/quality` - Data quality metrics

#### SCADA Data
- `GET /api/v1/scada/parameters` - Process parameters
- `GET /api/v1/scada/status` - Equipment status
- `GET /api/v1/scada/alerts` - System alerts

#### Weather Data
- `GET /api/v1/weather/current` - Current conditions
- `GET /api/v1/weather/forecast` - Weather forecast
- `GET /api/v1/weather/historical` - Historical data

### Analytics APIs

#### Performance Metrics
- `GET /api/v1/analytics/kpis` - Key performance indicators
- `GET /api/v1/analytics/trends` - Trend analysis
- `GET /api/v1/analytics/comparisons` - Comparative analysis

#### Predictive Analytics
- `GET /api/v1/predictions/density` - Algae density predictions
- `GET /api/v1/predictions/harvest` - Harvest timing predictions
- `GET /api/v1/predictions/optimization` - Optimization recommendations

### System APIs

#### Health & Status
- `GET /api/v1/health` - System health check
- `GET /api/v1/status` - System status
- `GET /api/v1/metrics` - System metrics

#### Configuration
- `GET /api/v1/config` - System configuration
- `PUT /api/v1/config` - Update configuration
- `GET /api/v1/config/schema` - Configuration schema

## Authentication

### API Keys
- Generate API keys through the admin interface
- Include API key in request headers: `Authorization: Bearer <api_key>`
- API keys have expiration dates and can be revoked

### Rate Limiting
- 1000 requests per hour per API key
- Rate limit headers included in responses
- Exceeded limits return HTTP 429

## Response Format

### Success Response
```json
{
  "status": "success",
  "data": {
    // Response data
  },
  "metadata": {
    "timestamp": "2024-01-01T00:00:00Z",
    "version": "1.0.0"
  }
}
```

### Error Response
```json
{
  "status": "error",
  "error": {
    "code": "INVALID_REQUEST",
    "message": "Invalid request parameters",
    "details": {
      // Additional error details
    }
  },
  "metadata": {
    "timestamp": "2024-01-01T00:00:00Z",
    "version": "1.0.0"
  }
}
```

## Data Models

### FlowCam Data Model
```json
{
  "date": "2024-01-01",
  "tpu_id": 1,
  "reactor_id": 1,
  "algae_density": 1.234,
  "measurement_count": 5,
  "quality_score": 0.95
}
```

### SCADA Data Model
```json
{
  "timestamp": "2024-01-01T00:00:00Z",
  "parameter_name": "temperature",
  "value": 25.5,
  "unit": "celsius",
  "status": "normal"
}
```

### Weather Data Model
```json
{
  "date": "2024-01-01",
  "temperature": 22.5,
  "humidity": 65.0,
  "sunlight_hours": 8.5,
  "precipitation": 0.0
}
```

## Query Parameters

### Filtering
- `start_date`: Start date for data range
- `end_date`: End date for data range
- `tpu_id`: Filter by TPU ID
- `reactor_id`: Filter by reactor ID
- `quality_threshold`: Minimum quality score

### Pagination
- `page`: Page number (default: 1)
- `limit`: Items per page (default: 100, max: 1000)
- `offset`: Number of items to skip

### Sorting
- `sort_by`: Field to sort by
- `sort_order`: Sort order (asc/desc)

## Examples

### Get FlowCam Data
```bash
curl -H "Authorization: Bearer <api_key>" \
  "https://api.baralgae.com/v1/flowcam/data?start_date=2024-01-01&end_date=2024-01-31&tpu_id=1"
```

### Get Performance Metrics
```bash
curl -H "Authorization: Bearer <api_key>" \
  "https://api.baralgae.com/v1/analytics/kpis?period=monthly"
```

### Get System Status
```bash
curl -H "Authorization: Bearer <api_key>" \
  "https://api.baralgae.com/v1/health"
```

## SDKs

### Python SDK
```python
from baralgae_api import BarAlgaeClient

client = BarAlgaeClient(api_key="your_api_key")
data = client.flowcam.get_data(start_date="2024-01-01", end_date="2024-01-31")
```

### JavaScript SDK
```javascript
import { BarAlgaeClient } from '@baralgae/api-client';

const client = new BarAlgaeClient({ apiKey: 'your_api_key' });
const data = await client.flowcam.getData({
  startDate: '2024-01-01',
  endDate: '2024-01-31'
});
```

## Support

For API support or questions:
- **Email**: api-support@baralgae.com
- **Documentation**: [API Docs](https://docs.baralgae.com/api)
- **Status Page**: [API Status](https://status.baralgae.com)
