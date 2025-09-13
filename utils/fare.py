# utils/fare.py
from datetime import datetime

def get_user_fare_profile(user):
    """
    Return passenger_type ('regular' or 'discount') and discount rate (0..100).
    Logic: if verified, not expired, and has a rate > 0 → discount.
    """
    now = datetime.utcnow()
    typ  = (user.eligibility_type or 'none')
    rate = int(getattr(user, 'discount_rate_pcnt', 0) or 0)

    ok_time = True
    if getattr(user, 'eligibility_verified_at', None) is None:
        ok_time = False
    if getattr(user, 'eligibility_expires_at', None):
        ok_time = ok_time and (user.eligibility_expires_at > now)

    if typ != 'none' and rate > 0 and ok_time:
        return ('discount', rate)
    return ('regular', 0)


def apply_fare(base_pesos: int, rate_pcnt: int) -> int:
    """
    Always return whole pesos; your app is already pesos-only.
    """
    if rate_pcnt <= 0:
        return int(base_pesos)
    discounted = round(base_pesos * (100 - rate_pcnt) / 100.0)
    return int(discounted)
