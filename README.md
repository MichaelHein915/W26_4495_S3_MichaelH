# W26_4495_S3_MichaelH
Applied Research Project/ Data Engineering

## Dashboard (HTML/CSS/JS frontend)

The dashboard uses a vanilla HTML/CSS/JavaScript frontend with a Flask API backend that consumes Kafka.

**Run the dashboard:**
```bash
# From project root
pip install -r requirements.txt
python Implementation/dashboard/api_server.py
```

Open http://localhost:5000 in your browser.

**Environment:**
- Set `DASHBOARD_PORT` (default: 5000) to run on a different port
- Uses the same `.env` config (Kafka, topic, etc.) as the rest of the pipeline
