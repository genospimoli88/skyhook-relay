# Skyhook Relay API

## Job Submission
- **Endpoint**: `POST /job`
- **Payload**: 
  ```json
  {
    "job_id": "unique_id",
    "job_type": "weather|text_classification|summarization|instant",
    "input_url": "https://example.com/input",
    "webhook_url": "https://user.com/webhook",
    "price": 0.12
  }
