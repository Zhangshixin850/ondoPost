import asyncio
import contextlib
import copy
import io
import logging
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import postOnly as app
from test_bot import fixture, until


class BookTimestampTests(unittest.TestCase):
    def setUp(self):
        self.exchange, self.bot, self.state = fixture()
        self.addCleanup(self.exchange.close)
        self.epoch = datetime(2026, 11, 1, 2, 48, 15, tzinfo=timezone.utc).timestamp()

    def test_rfc3339_variable_fraction_on_python310_and_newer(self):
        for digits in ('', '1', '12', '123', '1234', '12345', '123456',
                       '1234567', '12345678', '123456789'):
            with self.subTest(digits=digits), patch.object(app.time, 'time', return_value=self.epoch + .9):
                self.state.book_time = self.state.depth_time = 0
                raw = copy.deepcopy(self.exchange.book)
                raw['time'] = '2026-11-01T02:48:15' + ('.' + digits if digits else '') + 'Z'
                self.bot.set_book(self.state, raw)
                expected = self.epoch + int(digits[:6].ljust(6, '0') or '0') / 1000000
                self.assertAlmostEqual(self.state.depth_time, expected, places=6)
                self.assertEqual(self.bot.quote_targets(self.state)['buy'], app.Decimal('99.97'))

    def test_timezone_offset_and_nanosecond_top_book(self):
        raw = copy.deepcopy(self.exchange.book)
        raw['time'] = '2026-11-01T10:48:15.361987949+08:00'
        self.state.book_time = self.state.depth_time = 0
        with patch.object(app.time, 'time', return_value=self.epoch + .5):
            self.bot.set_book(self.state, raw, depth=False)
        self.assertAlmostEqual(self.state.book_time, self.epoch + .361987, places=6)

    def test_missing_timezone_or_invalid_fraction_still_rejected(self):
        for timestamp in (None, 123, 'bad-time', '2026-11-01T02:48:15.123456',
                          '2026-11-01T02:48:15.1234567890Z'):
            raw = copy.deepcopy(self.exchange.book)
            raw['time'] = timestamp
            with self.subTest(timestamp=timestamp), self.assertRaisesRegex(ValueError, 'RFC3339'):
                self.bot.set_book(self.state, raw)

    def test_old_and_future_nanosecond_books_keep_freshness_guards(self):
        raw = copy.deepcopy(self.exchange.book)
        self.state.book_time = self.state.depth_time = 0
        with patch.object(app.time, 'time', return_value=self.epoch + 10):
            raw['time'] = '2026-11-01T02:48:15.123456789Z'
            self.bot.set_book(self.state, raw)
            self.assertEqual(self.state.depth_time, 0)
        with patch.object(app.time, 'time', return_value=self.epoch):
            raw['time'] = '2026-11-01T02:48:25.123456789Z'
            with self.assertRaisesRegex(ValueError, '时间超前'):
                self.bot.set_book(self.state, raw)


class FeedDiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.exchange, self.bot, self.state = fixture()
        self.addCleanup(self.exchange.close)
        disabled = logging.root.manager.disable
        logging.disable(logging.NOTSET)
        self.addCleanup(logging.disable, disabled)

    async def test_bad_book_reports_channel_market_reason_and_location(self):
        async def stream(markets):
            yield {'type': 'ready'}
            self.bot.request_stop()
            yield {'type': 'update', 'channel': 'depthBooksPerps', 'data': [{
                'market': self.state.market['id'], 'time': 'invalid timestamp'}]}
        with patch.object(self.exchange, 'stream', stream), \
             self.assertLogs(app.log, level='WARNING') as logs:
            await self.bot.feed_loop()
        text = '\n'.join(logs.output)
        self.assertIn('ValueError:', text)
        self.assertIn('RFC3339', text)
        self.assertIn('channel=depthBooksPerps', text)
        self.assertIn('market=TEST-USD.P', text)
        self.assertIn('source=postOnly.py:', text)
        self.assertFalse(self.bot.ws_ready)
        self.assertFalse(self.bot.quote_allowed(self.state))

    async def test_stream_exception_redacts_credentials(self):
        async def stream(markets):
            yield {'type': 'ready'}
            self.bot.request_stop()
            raise ValueError('invalid time test_secret test_key')
        with patch.object(self.exchange, 'stream', stream), \
             self.assertLogs(app.log, level='WARNING') as logs:
            await self.bot.feed_loop()
        text = '\n'.join(logs.output)
        self.assertIn('<redacted>', text)
        self.assertNotIn('test_secret', text)
        self.assertNotIn('test_key', text)

    async def test_healthy_risk_does_not_claim_quotes_active_when_disconnected(self):
        self.bot.ws_ready = False
        output = io.StringIO()
        with patch.object(app, 'RISK_SECONDS', .01), contextlib.redirect_stdout(output):
            task = asyncio.create_task(self.bot.risk_loop())
            try:
                await until(lambda: '连接/同步中' in output.getvalue())
                self.assertNotIn('主单=监控中', output.getvalue())
            finally:
                self.bot.request_stop()
                await task
