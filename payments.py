import os
import stripe

stripe.api_key = os.getenv("STRIPE_API_KEY")

def create_payment_intent(amount_cents, currency="usd"):
    return stripe.PaymentIntent.create(
        amount=amount_cents,
        currency=currency,
        payment_method_types=["card"]
    )

def verify_payment_intent(payment_intent_id):
    intent = stripe.PaymentIntent.retrieve(payment_intent_id)
    return intent.status == "succeeded"

def refund_payment_intent(payment_intent_id):
    return stripe.Refund.create(payment_intent=payment_intent_id)
