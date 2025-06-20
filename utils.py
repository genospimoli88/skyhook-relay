import stripe, logging, os, json, boto3
from botocore.exceptions import NoCredentialsError

stripe.api_key = os.getenv("STRIPE_API_KEY")
logger = logging.getLogger("skyhook")

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)
S3_BUCKET = os.getenv("AWS_S3_BUCKET", "skyhook-results")

class PaymentError(Exception):
    pass

def get_job_price(job_type):
    pricing = {"weather": 0.25, "text": 0.10, "image": 0.50}
    if job_type not in pricing:
        raise ValueError(f"Unsupported job type: {job_type}")
    return pricing[job_type]

def charge_payment(amount, token):
    try:
        charge = stripe.Charge.create(
            amount=int(amount * 100),
            currency="usd",
            source=token,
            description=f"Skyhook job relay: ${amount:.2f}"
        )
        return charge.id
    except Exception as e:
        raise PaymentError(str(e))

def send_alert(message):
    logger.error(f"⚠️ ALERT: {message}")

def upload_to_s3(content, filename):
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=filename, Body=content)
        return f"https://{S3_BUCKET}.s3.amazonaws.com/{filename}"
    except NoCredentialsError as e:
        raise RuntimeError("S3 credentials error") from e
