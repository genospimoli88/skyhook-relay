import os
import stripe

# Set your Stripe API key from environment variable
stripe.api_key = os.getenv("STRIPE_API_KEY")

def create_payment_intent(amount_cents, currency="usd"):
    """
    Create a new Stripe PaymentIntent with specified amount and currency.
    amount_cents: int, amount in cents (e.g., 500 = $5.00)
    currency: str, currency code (default 'usd')
    Returns Stripe PaymentIntent object.
    """
    try:
        intent = stripe.PaymentIntent.create(
            amount=amount_cents,
            currency=currency,
            payment_method_types=["card"]
        )
        return intent
    except stripe.error.StripeError as e:
        # Log or handle the error as needed
        raise RuntimeError(f"Stripe error creating payment intent: {e}")

def verify_payment_intent(payment_intent_id):
    """
    Verify if the given PaymentIntent ID has succeeded.
    Returns True if payment succeeded, False otherwise.
    """
    try:
        intent = stripe.PaymentIntent.retrieve(payment_intent_id)
        return intent.status == "succeeded"
    except stripe.error.StripeError as e:
        # Log or handle the error as needed
        return False

def refund_payment_intent(payment_intent_id):
    """
    Create a refund for the given PaymentIntent ID.
    Returns Stripe Refund object.
    """
    try:
        refund = stripe.Refund.create(payment_intent=payment_intent_id)
        return refund
    except stripe.error.StripeError as e:
        # Log or handle the error as needed
        raise RuntimeError(f"Stripe error creating refund: {e}")
