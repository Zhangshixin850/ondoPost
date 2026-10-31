import concurrent.futures
import json
import threading
import time
import unittest
from email.utils import formatdate
from unittest.mock import Mock, patch

import requests
import ondo_exchange as adapter
from ondo_exchange import OndoAPIError, OndoExchange, OndoRequestSkipped, RequestLimiter


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def wait(self, seconds):
        self.now += seconds


def response(status=200, payload=None, headers=None):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(payload if payload is not None else
                                 {"success": True, "result": []}).encode()
    result.headers.update(headers or {})
    return result


class LimiterTests(unittest.TestCase):
    def assert_rate(self, sent, rps):
        for timestamp in sent:
            self.assertLessEqual(sum(timestamp - 1 + 1e-8 < t <= timestamp for t in sent), rps)
        self.assertTrue(all(b - a >= 1 / rps - 1e-8 for a, b in zip(sent, sent[1:])))

    def test_rps_one_five_ten_are_smooth_and_bounded(self):
        for rps in (1, 5, 10):
            with self.subTest(rps=rps):
                clock = Clock()
                limiter = RequestLimiter(rps, clock=clock, wait=clock.wait)
                sent = []
                for i in range(50):
                    limiter.acquire(priority=0 if i % 5 == 0 else 2)
                    sent.append(clock())
                self.assert_rate(sent, rps)

    def test_ordinary_requests_leave_one_slot_for_urgent_work(self):
        clock = Clock()
        limiter = RequestLimiter(5, clock=clock, wait=clock.wait)
        for _ in range(4):
            limiter.acquire(priority=2)
        limiter.acquire(priority=0)
        self.assertGreaterEqual(clock(), .8)
        self.assertLess(clock(), .81)
        limiter.acquire(priority=2)
        self.assertGreaterEqual(clock(), 1)

    def test_invalidated_request_does_not_consume_a_slot(self):
        clock = Clock()
        limiter = RequestLimiter(1, clock=clock, wait=clock.wait)
        limiter.acquire()
        allowed = True

        def invalidate(seconds):
            nonlocal allowed
            clock.wait(seconds)
            allowed = False

        limiter.wait = invalidate
        with self.assertRaises(OndoRequestSkipped):
            limiter.acquire(valid=lambda: allowed)
        self.assertEqual(len(limiter.sent), 1)
        self.assertFalse(limiter.waiters)

    def test_stopping_limiter_interrupts_long_cooldown(self):
        clock = Clock()
        limiter = RequestLimiter(5, clock=clock)
        limiter.defer(3600)
        def stop(seconds):
            clock.wait(seconds)
            limiter.stop()
        limiter.wait = stop
        with self.assertRaises(OndoRequestSkipped):
            limiter.acquire()
        self.assertFalse(limiter.sent)
        self.assertFalse(limiter.waiters)

    def test_concurrent_clients_share_one_budget(self):
        limiter = RequestLimiter(10)
        stamps = []
        lock = threading.Lock()

        def send(_):
            limiter.acquire()
            with lock:
                stamps.append(time.monotonic())

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(send, range(12)))
        self.assertEqual(len(stamps), 12)
        self.assert_rate(sorted(stamps), 10)

    def test_urgent_request_overtakes_blocked_background_request(self):
        limiter = RequestLimiter(2)
        limiter.acquire(priority=3)
        order = []
        background = threading.Thread(target=lambda: (limiter.acquire(3), order.append("background")))
        urgent = threading.Thread(target=lambda: (limiter.acquire(0), order.append("urgent")))
        background.start()
        deadline = time.monotonic() + 1
        while not limiter.waiters and time.monotonic() < deadline:
            time.sleep(.001)
        urgent.start()
        urgent.join(3)
        background.join(3)
        self.assertEqual(order, ["urgent", "background"])

    def test_backoff_is_shared_and_does_not_burst_after_cooldown(self):
        clock = Clock()
        limiter = RequestLimiter(5, clock=clock, wait=clock.wait)
        for expected in (2, 4, 8, 16, 30, 30):
            delay = limiter.defer()
            self.assertAlmostEqual(delay, expected)
            generation = limiter.cooldown_generation
            clock.now += .1
            limiter.defer()
            self.assertEqual(limiter.cooldown_generation, generation)
            clock.now = limiter.cooldown_until + .01
        limiter.defer(60)
        blocked_until = limiter.cooldown_until
        limiter.acquire(0)
        self.assertGreaterEqual(clock(), blocked_until)
        first = clock()
        limiter.acquire(0)
        self.assertGreaterEqual(clock() - first, .2 - 1e-8)
        clock.now += 61
        self.assertEqual(limiter.defer(), 2)


class CategoryLimiterTests(unittest.TestCase):
    def test_independent_smooth_write_budgets_and_read_bucket(self):
        account = adapter.AccountLimiter()
        clock = Clock()
        for bucket in account.buckets.values():
            bucket.clock = clock
            bucket.wait = clock.wait
        place = account.bucket('POST', '/v1/perps/orders/batch')
        cancel = account.bucket('DELETE', '/v1/perps/orders/batch')
        read = account.bucket('GET', '/v1/perps/positions')
        self.assertIsNot(place, cancel)
        place.acquire()
        cancel.acquire()
        read.acquire()
        self.assertEqual(clock(), 0)
        place.acquire()
        self.assertAlmostEqual(clock(), .5)
        cancel.acquire()
        self.assertAlmostEqual(clock(), .5)
        place.defer(10)
        cancel.acquire()
        self.assertLess(clock(), 2)
        self.assertFalse(cancel.cooling)
        self.assertTrue(place.cooling)

    def test_transport_429_only_cools_failed_category(self):
        account = adapter.AccountLimiter()
        clock = Clock()
        for bucket in account.buckets.values():
            bucket.clock = clock
            bucket.wait = clock.wait
        client = OndoExchange('fake-key', 'fake-secret', limiter=account)
        self.addCleanup(client.close)
        client._session.send = Mock(side_effect=[response(429, {'success':False}, {'Retry-After':'5'}),
                                                  response()])
        with self.assertRaises(OndoAPIError):
            client._request('POST', '/v1/perps/orders/batch', body={'orders':[]})
        client._request('DELETE', '/v1/perps/orders/batch', params={'orderIDs':'1'})
        self.assertEqual(clock(), 0)
        self.assertTrue(account.buckets['place'].cooling)
        self.assertFalse(account.buckets['cancel'].cooling)


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.limiter = RequestLimiter(5, clock=self.clock, wait=self.clock.wait)
        self.client = OndoExchange("fake-key", "fake-secret", limiter=self.limiter)
        self.addCleanup(self.client.close)
        self.client._session.send = Mock(return_value=response())

    def test_all_adapter_instances_use_the_same_transport_limiter(self):
        other = OndoExchange("fake-key", "fake-secret", limiter=self.limiter)
        self.addCleanup(other.close)
        other._session.send = Mock(return_value=response())
        for i in range(15):
            client = self.client if i % 2 else other
            client._request("GET", "/v1/perps/balance")
        self.assertGreaterEqual(self.clock(), 3)
        self.assertEqual(self.client._session.send.call_count + other._session.send.call_count, 15)

    def test_numeric_retry_after_blocks_other_client(self):
        self.client._session.send.return_value = response(
            429, {"success": False}, {"Retry-After": "5"},
        )
        with self.assertRaises(OndoAPIError) as caught:
            self.client._request("POST", "/v1/perps/orders", body={})
        self.assertEqual(caught.exception.status, 429)
        self.assertEqual(caught.exception.retry_after, 5)
        self.assertFalse(caught.exception.uncertain)
        other = OndoExchange("fake-key", "fake-secret", limiter=self.limiter)
        self.addCleanup(other.close)
        other._session.send = Mock(return_value=response())
        other._request("GET", "/v1/perps/balance")
        self.assertGreaterEqual(self.clock(), 5)

    def test_http_date_retry_after(self):
        self.client._session.send.return_value = response(
            429, {"success": False}, {"Retry-After": formatdate(1700000006, usegmt=True)},
        )
        with patch.object(adapter.time, "time", return_value=1700000000):
            with self.assertRaises(OndoAPIError) as caught:
                self.client._request("GET", "/v1/perps/balance")
        self.assertEqual(caught.exception.retry_after, 6)

    def test_missing_retry_after_uses_shared_two_second_cooldown(self):
        self.client._session.send.return_value = response(429, {"success": False})
        with self.assertRaises(OndoAPIError):
            self.client._request("GET", "/v1/perps/balance")
        self.assertEqual(self.limiter.cooldown_until, 2)

    def test_signature_timestamp_is_created_after_waiting(self):
        self.limiter.defer(15)
        with patch.object(adapter.time, "time", side_effect=lambda: 1700000000 + self.clock()):
            self.client._request("GET", "/v1/perps/balance")
        prepared = self.client._session.send.call_args.args[0]
        self.assertGreaterEqual(self.clock(), 15)
        self.assertEqual(prepared.headers["ONDO-TIMESTAMP"], str(int((1700000000 + self.clock()) * 1000)))

    def test_expired_primary_request_never_reaches_transport(self):
        with self.client.request_context(post_valid=lambda: False):
            with self.assertRaises(OndoRequestSkipped):
                self.client._request("POST", "/v1/perps/orders", body={})
        self.client._session.send.assert_not_called()
        self.assertFalse(self.limiter.sent)


if __name__ == "__main__":
    unittest.main()
