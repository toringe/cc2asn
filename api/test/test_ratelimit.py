import unittest

from lib.ratelimit import RateLimiter


class TestRateLimiter(unittest.TestCase):
    def test_within_limits_true(self):
        limiter = RateLimiter(per_minute=3, per_day=10)
        self.assertTrue(limiter.allow("1.2.3.4", "2026-10-30", now=1000.0))

    def test_minute_burst_rejected(self):
        limiter = RateLimiter(per_minute=3, per_day=10)
        results = [limiter.allow("1.2.3.4", "2026-10-30", now=1000.0 + i) for i in range(4)]
        self.assertEqual(results, [True, True, True, False])

    def test_minute_window_expires(self):
        limiter = RateLimiter(per_minute=3, per_day=10)
        for i in range(3):
            limiter.allow("1.2.3.4", "2026-10-30", now=1000.0 + i)
        self.assertFalse(limiter.allow("1.2.3.4", "2026-10-30", now=1030.0))
        self.assertTrue(limiter.allow("1.2.3.4", "2026-10-30", now=1061.0))

    def test_daily_cap_reached_across_minutes(self):
        limiter = RateLimiter(per_minute=10, per_day=5)
        for i in range(5):
            self.assertTrue(limiter.allow("1.2.3.4", "2026-10-30", now=1000.0 + i * 30))
        self.assertFalse(limiter.allow("1.2.3.4", "2026-10-30", now=1200.0))

    def test_day_counter_resets(self):
        limiter = RateLimiter(per_minute=10, per_day=5)
        for i in range(5):
            limiter.allow("1.2.3.4", "2026-10-30", now=1000.0 + i)
        self.assertFalse(limiter.allow("1.2.3.4", "2026-10-30", now=1100.0))
        self.assertTrue(limiter.allow("1.2.3.4", "2026-10-31", now=2000.0))

    def test_buckets_do_not_grow_unboundedly(self):
        limiter = RateLimiter(per_minute=100, per_day=1000)
        for i in range(1000):
            ip = f"10.0.0.{i % 6}"  # same six IPs churn through windows
            limiter.allow(ip, "2026-10-30", now=1000.0 + i * 61)
        self.assertLessEqual(len(limiter._buckets), 12)
        for ip, bucket in limiter._buckets.items():
            self.assertLessEqual(len(bucket["stamps"]), 3)  # only in-window stamps tallied


if __name__ == "__main__":
    unittest.main()
