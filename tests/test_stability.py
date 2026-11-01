"""离线盘口回放：稳定锚、同步宽限和静默深度核验，不访问交易所。"""
import asyncio
import copy
import time
import types
import unittest
from decimal import Decimal as D
from unittest.mock import AsyncMock, Mock, patch

import postOnly as app
import ondo_exchange as adapter
from test_bot import fixture, push_book, until
from test_rate_limit import Clock, response


class AnchorTests(unittest.TestCase):
    def setUp(self):
        self.exchange, self.bot, self.state = fixture()
        self.addCleanup(self.exchange.close)
        self.clock = [time.monotonic()]
        self.time_patch = patch.object(app, 'time', types.SimpleNamespace(
            monotonic=lambda: self.clock[0], time=time.time))
        self.time_patch.start()
        self.addCleanup(self.time_patch.stop)
        self.bot.config.wall_confirm_ms = D(500)

    def snapshot(self, quantity='5', anchor='20'):
        book = copy.deepcopy(self.exchange.book)
        book['bids'][1][1] = anchor
        book['bids'][2][1] = quantity
        push_book(self.exchange, self.bot, self.state, book)
        return self.bot.quote_targets(self.state)

    def enter(self):
        self.assertIsNone(self.snapshot()['buy'])
        self.clock[0] += .51
        self.assertEqual(self.snapshot()['buy'], D('99.97'))

    def test_entry_requires_elapsed_time_and_a_new_sample(self):
        self.snapshot()
        self.clock[0] += .6
        self.assertIsNone(self.bot.quote_targets(self.state)['buy'])
        self.assertEqual(self.snapshot()['buy'], D('99.97'))

    def test_advantage_below_25_percent_never_changes_anchor(self):
        self.enter()
        for i in range(60):
            self.clock[0] += 1
            self.assertEqual(self.snapshot('24' if i % 2 else '19')['buy'], D('99.97'))
        self.assertIsNone(self.state.anchors['buy']['candidate'])

    def test_transient_leader_and_advantage_reset_confirmation(self):
        self.enter()
        self.snapshot('30')
        self.clock[0] += .4
        self.assertEqual(self.snapshot('24')['buy'], D('99.97'))
        self.clock[0] += .2
        self.assertEqual(self.snapshot('30')['buy'], D('99.97'))
        self.clock[0] += .4
        self.assertEqual(self.snapshot('30')['buy'], D('99.97'))
        self.clock[0] += .11
        self.assertEqual(self.snapshot('30')['buy'], D('99.95'))

    def test_no_timer_only_confirmation_after_early_second_sample(self):
        self.enter()
        self.snapshot('30')
        self.clock[0] += .1
        self.snapshot('30')
        self.clock[0] += .5
        self.assertEqual(self.bot.quote_targets(self.state)['buy'], D('99.97'))
        self.assertEqual(self.snapshot('30')['buy'], D('99.95'))

    def test_strongest_wall_uses_same_level_after_confirmation(self):
        book = copy.deepcopy(self.exchange.book)
        book['bids'].append(['99.95', '200'])
        push_book(self.exchange, self.bot, self.state, book)
        self.bot.quote_targets(self.state)
        self.clock[0] += .6
        push_book(self.exchange, self.bot, self.state, book)
        self.assertEqual(self.bot.quote_targets(self.state)['buy'], D('99.95'))
        self.assertEqual(self.state.anchors['buy']['mode'], 'same_level')

    def test_selection_mid_comes_from_depth_safety_from_latest_bbo(self):
        self.enter()
        snapshot_mid = self.state.depth_mid
        self.state.book = (D('99.98'), D('100.02'))
        self.assertEqual(self.bot.quote_targets(self.state)['buy'], D('99.97'))
        self.assertEqual(self.state.depth_mid, snapshot_mid)
        self.state.book = (D('99.97'), D('100.01'))
        self.assertIsNone(self.bot.quote_targets(self.state)['buy'])
        self.assertEqual(self.state.anchors['buy']['reason'], 'self_best')

    def test_newer_bbo_does_not_change_wall_scan_window(self):
        book = copy.deepcopy(self.exchange.book)
        book['bids'].append(['99.95', '200'])
        push_book(self.exchange, self.bot, self.state, book)
        self.state.book = (D('100'), D('100.02'))
        self.assertIsNone(self.bot.quote_targets(self.state)['buy'])
        self.assertEqual(self.state.anchors['buy']['largest'], D('99.95'))

    def test_equal_timestamp_packet_cannot_clear_order_barrier(self):
        raw = push_book(self.exchange, self.bot, self.state)
        before = self.state.depth_seq
        at = self.state.depth_at
        self.state.depth_barrier = before
        self.clock[0] += 1
        raw['bids'][1][1] = '9999'
        self.bot.set_book(self.state, raw)
        self.assertEqual(self.state.depth_seq, before)
        self.assertEqual(self.state.depth_at, at)
        self.assertEqual(self.state.depth[0][D('99.98')], D(20))
        self.assertEqual(self.state.ws_received_at, self.clock[0])


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.exchange, self.bot, self.state = fixture()
        self.addCleanup(self.exchange.close)
        self.clock = [time.monotonic()]
        self.time_patch = patch.object(app, 'time', types.SimpleNamespace(
            monotonic=lambda: self.clock[0], time=time.time))
        self.time_patch.start()
        self.addCleanup(self.time_patch.stop)
        await self.bot.place_quotes(self.state)
        push_book(self.exchange, self.bot, self.state)
        self.bot.config.wall_confirm_ms = D(500)
        self.bot.ws_alive_at = self.clock[0]
        self.worker = asyncio.create_task(self.bot.run_market(self.state))
        self.addAsyncCleanup(self.finish)
        await asyncio.sleep(.05)

    async def finish(self):
        self.bot.request_stop()
        await self.worker

    async def frame(self, delta=.1, book=None, own=True):
        self.clock[0] += delta
        self.bot.ws_alive_at = self.clock[0]
        push_book(self.exchange, self.bot, self.state, book, own=own)
        await asyncio.sleep(.05)

    def deletes(self):
        return sum(method == 'DELETE' and path.endswith('/batch')
                   for method, path, body in self.exchange.calls)

    async def test_one_minute_of_alternating_small_leaders_keeps_both_orders(self):
        original = dict(self.state.main)
        for i in range(60):
            book = copy.deepcopy(self.exchange.book)
            book['bids'][2][1] = '24' if i % 2 else '19'
            book['asks'][2][1] = '24' if i % 2 else '19'
            await self.frame(1, book)
        self.assertEqual(self.state.main, original)
        self.assertEqual(self.deletes(), 0)
        self.assertEqual(len(self.exchange.posts()), 2)

    async def test_sustained_large_challenger_switches_only_once(self):
        sell = self.state.main['sell']
        book = copy.deepcopy(self.exchange.book)
        book['bids'][2][1] = '30'
        await self.frame(.1, book)
        await self.frame(.4, book)
        self.assertEqual(self.deletes(), 0)
        await self.frame(.11, book)
        await until(lambda: 'buy' not in self.state.main)
        self.assertEqual(self.deletes(), 1)
        await self.frame(.1, book)
        await until(lambda: 'buy' in self.state.main)
        buy = self.state.tracked[self.state.main['buy']]
        self.assertEqual(buy['price'], D('99.95'))
        self.assertEqual(self.state.main['sell'], sell)
        for _ in range(5):
            await self.frame(.1, book)
        self.assertEqual(self.deletes(), 1)
        self.assertEqual(len(self.exchange.posts()), 3)

    async def test_wall_disappearing_cancels_immediately_and_reentry_confirms(self):
        book = copy.deepcopy(self.exchange.book)
        book['bids'].pop(1)
        await self.frame(.01, book)
        await until(lambda: 'buy' not in self.state.main)
        self.assertIn('sell', self.state.main)
        await self.frame(.01, book)
        self.assertNotIn('buy', self.state.main)
        await self.frame(.51, book)
        await until(lambda: 'buy' in self.state.main)
        self.assertEqual(len(self.exchange.posts()), 3)

    async def test_out_of_band_cancels_without_waiting_for_confirmation(self):
        self.state.book = (D('100.01'), D('100.05'))
        self.state.wake.set()
        await until(lambda: 'buy' not in self.state.main)
        self.assertFalse(self.state.main)

    async def test_own_order_delay_holds_safe_side_then_recovers_without_cancel(self):
        original = dict(self.state.main)
        await self.frame(.1, own=False)
        self.assertEqual(self.state.blocked_sides, {'buy', 'sell'})
        self.assertEqual(self.deletes(), 0)
        await self.frame(.5)
        self.assertFalse(self.state.blocked_sides)
        self.assertEqual(self.state.main, original)
        self.assertEqual(self.deletes(), 0)

    async def test_sync_timeout_cancels_affected_side_only(self):
        buy = self.state.tracked[self.state.main['buy']]
        raw = copy.deepcopy(self.exchange.orders[buy['id']])
        raw['size'] = '100'
        raw['orderId'] = 'manual'
        raw['clientOrderId'] = 'manual'
        self.bot.observe_order(self.state, raw)
        await self.frame(.1)
        self.assertEqual(self.state.blocked_sides, {'buy'})
        self.assertEqual(self.deletes(), 0)
        await self.frame(1.01)
        await until(lambda: 'buy' not in self.state.main)
        self.assertIn('sell', self.state.main)
        self.assertEqual(self.deletes(), 1)

    async def test_unverifiable_anchor_cancels_immediately(self):
        raw = {'orderId': 'manual', 'clientOrderId': 'manual', 'market': self.state.market['id'],
               'side': 'buy', 'price': '99.98', 'size': '100', 'filledSize': '0', 'status': 'open'}
        self.bot.observe_order(self.state, raw)
        await self.frame(.01)
        await until(lambda: 'buy' not in self.state.main)
        self.assertIn('sell', self.state.main)

    async def test_metadata_only_updates_and_reconcile_do_not_move_barrier(self):
        before = self.state.orders_seq, self.state.depth_barrier
        raw = copy.deepcopy(next(iter(self.exchange.orders.values())))
        raw['updatedAt'] = 'new metadata'
        raw['size'] = str(D(raw['size']) + D('0.0000'))
        self.bot.observe_order(self.state, raw)
        self.assertEqual((self.state.orders_seq, self.state.depth_barrier), before)
        await self.bot.reconcile()
        self.assertEqual((self.state.orders_seq, self.state.depth_barrier), before)
        self.assertEqual(self.deletes(), 0)

    async def test_cancellation_log_records_reason_and_actual_new_quote_price(self):
        disabled = app.logging.root.manager.disable
        app.logging.disable(app.logging.NOTSET)
        self.addCleanup(app.logging.disable, disabled)
        self.state.anchors['buy'].update(price=D('99.96'), largest=D('99.96'), largest_amount=D(3000))
        with self.assertLogs(app.log, level='INFO') as logs:
            await self.bot.cancel_main(self.state, ('buy',), reason='wall_switch')
        text = '\n'.join(logs.output)
        for expected in ('reason=wall_switch', 'old=99.97', 'new=99.95', 'hold=', 'revision='):
            self.assertIn(expected, text)


class QuietBookTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.exchange, self.bot, self.state = fixture()
        self.addCleanup(self.exchange.close)
        await self.bot.place_quotes(self.state)
        self.raw = push_book(self.exchange, self.bot, self.state)
        self.bot.ws_alive_at = time.monotonic()
        self.state.depth_at -= 3.1
        self.worker = None
        self.addAsyncCleanup(self.finish)

    async def finish(self):
        self.bot.request_stop()
        if self.worker:
            await self.worker

    async def start(self):
        self.worker = asyncio.create_task(self.bot.run_market(self.state))
        await asyncio.sleep(.03)

    async def test_rest_validation_preserves_orders_and_blocks_new_during_probe(self):
        release = asyncio.Event()
        original = dict(self.state.main)
        async def probe(state):
            await release.wait()
            return self.raw
        with patch.object(self.bot, 'verify_quiet_book', side_effect=probe) as request:
            await self.start()
            self.assertTrue(self.state.quiet_deadline)
            self.assertEqual(self.state.main, original)
            self.assertEqual(request.call_count, 1)
            await self.bot.place_quotes(self.state)
            self.assertEqual(len(self.exchange.posts()), 2)
            release.set()
            await until(lambda: self.state.probe is None)
            self.assertEqual(self.state.main, original)
            self.assertEqual(self.state.quiet_deadline, 0)
            self.assertLess(time.monotonic()-self.state.depth_at, 1)

    async def test_deadline_cancels_while_read_still_in_flight_and_late_result_is_discarded(self):
        release = asyncio.Event()
        async def probe(state):
            await release.wait()
            return self.raw
        with patch.object(self.bot, 'verify_quiet_book', side_effect=probe) as request:
            await self.start()
            sequence = self.state.depth_seq
            self.state.probe_deadline = self.state.quiet_deadline = time.monotonic()-.01
            self.state.wake.set()
            await until(lambda: not self.state.main)
            self.assertEqual(request.call_count, 1)
            self.assertFalse(self.state.probe.done())
            release.set()
            await until(lambda: self.state.probe is None)
            self.assertEqual(self.state.depth_seq, sequence)
            self.assertEqual(len(self.exchange.posts()), 2)

    async def test_failed_probe_cancels_without_poisoning_account_state(self):
        with patch.object(self.bot, 'verify_quiet_book', side_effect=adapter.OndoAPIError('unavailable')):
            await self.start()
            await until(lambda: not self.state.main)
        self.assertFalse(self.state.problem)

    async def test_unverified_own_deduction_in_rest_cancels_only_affected_side(self):
        raw = copy.deepcopy(self.raw)
        raw['bids'] = copy.deepcopy(self.exchange.book['bids'])
        with patch.object(self.bot, 'verify_quiet_book', return_value=raw):
            await self.start()
            await until(lambda: 'buy' not in self.state.main)
            self.assertIn('sell', self.state.main)

    async def test_new_ws_snapshot_supersedes_late_probe_failure(self):
        release = asyncio.Event()
        async def probe(state):
            await release.wait()
            raise adapter.OndoAPIError('late failure')
        with patch.object(self.bot, 'verify_quiet_book', side_effect=probe):
            await self.start()
            push_book(self.exchange, self.bot, self.state)
            await until(lambda: not self.state.quiet_deadline)
            original = dict(self.state.main)
            release.set()
            await until(lambda: self.state.probe is None)
            self.assertEqual(self.state.main, original)

    async def test_connection_generation_or_own_revision_invalidates_result(self):
        for attribute in ('generation', 'orders_seq'):
            with self.subTest(attribute=attribute):
                exchange, bot, state = fixture()
                self.addCleanup(exchange.close)
                await bot.place_quotes(state)
                raw = push_book(exchange, bot, state)
                state.depth_at -= 3.1
                bot.ws_alive_at = time.monotonic()
                release = asyncio.Event()
                async def probe(current):
                    await release.wait()
                    return raw
                with patch.object(bot, 'verify_quiet_book', side_effect=probe):
                    worker = asyncio.create_task(bot.run_market(state))
                    try:
                        await until(lambda: state.probe is not None)
                        sequence = state.depth_seq
                        target = bot if attribute == 'generation' else state
                        setattr(target, attribute, getattr(target, attribute)+1)
                        release.set()
                        await until(lambda: not state.main)
                        self.assertEqual(state.depth_seq, sequence)
                    finally:
                        bot.request_stop()
                        release.set()
                        await worker

    async def test_probe_rejects_incomplete_100_level_band(self):
        raw = copy.deepcopy(self.raw)
        raw['bids'] = [['99.99', '1']]*100
        before = self.state.depth_seq
        with self.assertRaisesRegex(ValueError, '100'):
            self.bot.accept_probe(self.state, raw)
        self.assertEqual(self.state.depth_seq, before)

    async def test_probe_disagreement_with_newer_bbo_does_not_replace_depth(self):
        self.state.book = (D('99.98'), D('100.01'))
        self.state.book_time += .01
        before = self.state.depth_seq, self.state.depth_at
        with self.assertRaisesRegex(ValueError, 'REST'):
            self.bot.accept_probe(self.state, self.raw)
        self.assertEqual((self.state.depth_seq, self.state.depth_at), before)

    async def test_dead_heartbeat_cancels_without_probe(self):
        self.bot.ws_alive_at = time.monotonic()-16
        with patch.object(self.bot, 'verify_quiet_book', new_callable=AsyncMock) as request:
            await self.start()
            await until(lambda: not self.state.main)
            request.assert_not_called()


class ProbeTransportTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.limiter = adapter.RequestLimiter(5, clock=self.clock, wait=self.clock.wait)
        self.client = adapter.OndoExchange('test_key', 'test_secret', limiter=self.limiter)
        self.addCleanup(self.client.close)
        self.client._depth_session.send = Mock(return_value=response())
        self.client._session.send = Mock(return_value=response())
        self.clock_patch = patch.object(adapter.time, 'monotonic', self.clock)
        self.clock_patch.start()
        self.addCleanup(self.clock_patch.stop)

    def test_expired_in_read_queue_never_sends(self):
        self.limiter.defer(5)
        with self.client.request_context(priority=1, deadline=1):
            with self.assertRaises(adapter.OndoRequestSkipped):
                self.client._request('GET', '/v1/perps/depth')
        self.client._depth_session.send.assert_not_called()
        self.assertLessEqual(self.clock(), 1.1)

    def test_network_timeout_uses_remaining_budget_and_separate_session(self):
        self.limiter.acquire()
        with self.client.request_context(priority=1, deadline=1):
            self.client._request('GET', '/v1/perps/depth')
        self.client._session.send.assert_not_called()
        timeout = self.client._depth_session.send.call_args.kwargs['timeout']
        self.assertAlmostEqual(timeout, .8)


if __name__ == '__main__':
    unittest.main()
