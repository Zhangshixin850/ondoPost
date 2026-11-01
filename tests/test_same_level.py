"""同价边界挂单、外部保护门槛及模式切换的离线回放。"""
import asyncio
import copy
import time
import types
import unittest
from decimal import Decimal as D
from unittest.mock import patch

import postOnly as app
from ondo_exchange import OndoRequestSkipped
from test_bot import fixture, push_book, until


BOUNDARY = {'bids': [['99.99', '1'], ['99.95', '20']],
            'asks': [['100.01', '1'], ['100.05', '20']]}
# 买侧退后一档为99.95，初始中间价100.005时越界，降至100时恰好合规。
RESTORABLE = {'bids': [['99.99', '1'], ['99.96', '20']],
              'asks': [['100.02', '1'], ['100.05', '20']]}


class SameLevelTargetsTests(unittest.TestCase):
    def setUp(self):
        self.exchange, self.bot, self.state = fixture()
        self.addCleanup(self.exchange.close)

    def book(self, raw):
        push_book(self.exchange, self.bot, self.state, raw)
        return self.bot.quote_targets(self.state)

    def test_both_sides_fall_back_to_wall_at_exact_boundary(self):
        self.assertEqual(self.book(BOUNDARY), {'buy': D('99.95'), 'sell': D('100.05')})
        self.assertTrue(all(a['mode'] == 'same_level' for a in self.state.anchors.values()))

    def test_one_tick_behind_at_exact_boundary_keeps_behind_mode(self):
        raw = copy.deepcopy(BOUNDARY)
        raw['bids'][1][0] = '99.96'
        raw['asks'][1][0] = '100.04'
        self.assertEqual(self.book(raw), {'buy': D('99.95'), 'sell': D('100.05')})
        self.assertTrue(all(a['mode'] == 'behind' for a in self.state.anchors.values()))

    def test_same_level_never_bypasses_best_quote_rule(self):
        raw = {'bids': [['99.95', '20']], 'asks': [['100.05', '20']]}
        self.assertEqual(self.book(raw), {'buy': None, 'sell': None})

    def test_same_level_wall_itself_outside_latest_band_has_no_second_choice(self):
        push_book(self.exchange, self.bot, self.state, BOUNDARY)
        self.state.book = (D('100'), D('100.02'))
        self.assertIsNone(self.bot.quote_targets(self.state)['buy'])
        self.assertEqual(self.state.anchors['buy']['largest'], D('99.95'))

    def test_thin_strongest_wall_blocks_entry_without_second_choice(self):
        raw = copy.deepcopy(BOUNDARY)
        raw['bids'][1][1] = '4'
        raw['bids'].append(['99.98', '3'])
        self.assertIsNone(self.book(raw)['buy'])
        self.assertEqual(self.state.anchors['buy']['largest'], D('99.95'))
        self.assertNotEqual(self.bot.quote_targets(self.state)['sell'], None)

    def test_entry_threshold_uses_rounded_order_quantity(self):
        # 实际拟下单10.005个，门槛5.0025，不能用未舍入的1000/99.95计算。
        raw = copy.deepcopy(BOUNDARY)
        raw['bids'][1][1] = '5.0025'
        self.assertEqual(self.book(raw)['buy'], D('99.95'))

    def test_manual_order_cannot_supply_its_own_protection(self):
        order = self.exchange.create_order('TEST', 'limit', 'buy', '100', '99.95',
                                           {'clientOrderId': 'manual'})
        self.bot.observe_order(self.state, order['info'])
        raw = copy.deepcopy(BOUNDARY)
        raw['bids'][1][1] = '4'
        self.assertIsNone(self.book(raw)['buy'])

    def test_confirmation_resets_when_target_mode_changes(self):
        clock = [time.monotonic()]
        self.bot.config.wall_confirm_ms = D(500)
        with patch.object(app, 'time', types.SimpleNamespace(monotonic=lambda: clock[0], time=time.time)):
            self.assertIsNone(self.book(RESTORABLE)['buy'])
            clock[0] += .4
            valid = copy.deepcopy(RESTORABLE)
            valid['asks'][0][0] = '100.01'
            self.assertIsNone(self.book(valid)['buy'])
            clock[0] += .2
            self.assertIsNone(self.book(valid)['buy'])
            clock[0] += .31
            self.assertEqual(self.book(valid)['buy'], D('99.95'))
            self.assertEqual(self.state.anchors['buy']['mode'], 'behind')


class SameLevelWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.exchange, self.bot, self.state = fixture()
        self.addCleanup(self.exchange.close)
        self.clock = [time.monotonic()]
        self.clock_patch = patch.object(app, 'time', types.SimpleNamespace(
            monotonic=lambda: self.clock[0], time=time.time))
        self.clock_patch.start()
        self.addCleanup(self.clock_patch.stop)
        self.exchange.book = copy.deepcopy(RESTORABLE)
        push_book(self.exchange, self.bot, self.state)
        await self.bot.place_quotes(self.state)
        push_book(self.exchange, self.bot, self.state)
        self.bot.config.wall_confirm_ms = D(500)
        self.bot.ws_alive_at = self.clock[0]
        self.worker = None
        self.addAsyncCleanup(self.finish)

    async def finish(self):
        self.bot.request_stop()
        if self.worker:
            await self.worker
        else:
            await self.bot.cancel_main(self.state)

    async def start(self):
        self.worker = asyncio.create_task(self.bot.run_market(self.state))
        await asyncio.sleep(.05)

    async def frame(self, delta=.1, raw=None, own=True):
        self.clock[0] += delta
        self.bot.ws_alive_at = self.clock[0]
        push_book(self.exchange, self.bot, self.state, raw, own=own)
        await asyncio.sleep(.05)

    def entry(self, side='buy'):
        return self.state.tracked[self.state.main[side]]

    def deletes(self):
        return sum(m == 'DELETE' and p.endswith('/batch') for m, p, b in self.exchange.calls)

    def restore_book(self):
        raw = copy.deepcopy(RESTORABLE)
        raw['asks'][0][0] = '100.01'
        return raw

    async def test_submitted_orders_record_mode_and_keep_fixed_notional(self):
        for side, price in [('buy', D('99.96')), ('sell', D('100.05'))]:
            entry = self.entry(side)
            self.assertEqual((entry['price'], entry['quote_mode']), (price, 'same_level'))
            self.assertLessEqual(entry['size']*price, self.state.order_size)
            self.assertGreater(entry['size']*price, self.state.order_size-D('.11'))
        self.assertTrue(all(p['postOnly'] for p in self.exchange.posts()))

    async def test_external_half_is_inclusive_then_thinning_cancels_only_buy(self):
        original = dict(self.state.main)
        raw = copy.deepcopy(RESTORABLE)
        for side, key in [('buy', 'bids'), ('sell', 'asks')]:
            raw[key][1][1] = str(self.entry(side)['size']/2)
        await self.start()
        await self.frame(raw=raw)
        self.assertEqual(self.state.main, original)
        self.assertEqual(self.deletes(), 0)
        raw['bids'][1][1] = str(D(raw['bids'][1][1])-D('.0001'))
        await self.frame(.01, raw)
        await until(lambda: 'buy' not in self.state.main)
        self.assertEqual(self.state.main['sell'], original['sell'])
        self.assertEqual(self.deletes(), 1)

    async def test_only_own_order_left_is_not_external_protection(self):
        raw = copy.deepcopy(RESTORABLE)
        raw['bids'].pop(1)
        await self.start()
        await self.frame(.01, raw)
        await until(lambda: 'buy' not in self.state.main)
        self.assertIn('sell', self.state.main)

    async def test_manual_same_price_order_is_subtracted_and_preserved(self):
        manual = self.exchange.create_order('TEST', 'limit', 'sell', '100', '100.05',
                                            {'clientOrderId': 'manual'})
        self.bot.observe_order(self.state, manual['info'])
        raw = copy.deepcopy(RESTORABLE)
        raw['asks'][1][1] = '4'
        await self.start()
        await self.frame(.01, raw)
        await until(lambda: 'sell' not in self.state.main)
        self.assertIn('buy', self.state.main)
        self.assertEqual(self.exchange.orders[manual['id']]['status'], 'open')

    async def test_uncertain_anchor_cancels_without_sync_grace(self):
        await self.start()
        raw = copy.deepcopy(RESTORABLE)
        raw['bids'][1][1] = raw['asks'][1][1] = '1'
        await self.frame(.01, raw, own=False)
        await until(lambda: not self.state.main)

    async def test_pre_send_check_rejects_thin_wall_without_mutating_confirmation(self):
        await self.bot.cancel_main(self.state)
        self.bot.config.wall_confirm_ms = D(0)
        push_book(self.exchange, self.bot, self.state)
        original_rest = self.bot.rest
        checked = []
        async def rest(client, method, *args, **kwargs):
            if method == 'create_orders':
                raw = copy.deepcopy(RESTORABLE)
                raw['bids'][1][1] = '4'
                push_book(self.exchange, self.bot, self.state, raw)
                before = copy.deepcopy(self.state.anchors), copy.deepcopy(self.state.tracked)
                checked.append(kwargs['post_valid']())
                self.assertEqual((self.state.anchors, self.state.tracked), before)
                raise OndoRequestSkipped('protection changed while queued')
            return await original_rest(client, method, *args, **kwargs)
        with patch.object(self.bot, 'rest', side_effect=rest):
            await self.bot.place_quotes(self.state)
        self.assertEqual(checked, [False])
        self.assertEqual(len(self.exchange.posts()), 2)
        self.assertFalse(self.state.main)

    async def test_protection_check_is_read_only_and_uses_remaining_size(self):
        entry = self.entry()
        entry['filled'] = D('2')
        self.exchange.orders[entry['id']]['filledSize'] = '2'
        self.bot.observe_order(self.state, self.exchange.orders[entry['id']])
        raw = copy.deepcopy(RESTORABLE)
        raw['bids'][1][1] = str((entry['size']-entry['filled'])/2)
        push_book(self.exchange, self.bot, self.state, raw)
        anchors = copy.deepcopy(self.state.anchors)
        self.assertEqual(self.bot.quote_targets(self.state, update=False)['buy'], D('99.96'))
        self.assertEqual(self.state.anchors, anchors)
        raw['bids'][1][1] = str(D(raw['bids'][1][1])-D('.0001'))
        push_book(self.exchange, self.bot, self.state, raw)
        self.assertIsNone(self.bot.quote_targets(self.state, update=False)['buy'])
        self.assertEqual(self.state.anchors, anchors)

    async def test_restore_behind_confirms_once_without_reconfirming_entry(self):
        original = dict(self.state.main)
        raw = self.restore_book()
        await self.start()
        await self.frame(.1, raw)
        await self.frame(.4, raw)
        self.assertEqual(self.state.main, original)
        await self.frame(.11, raw)
        await until(lambda: 'buy' not in self.state.main)
        self.assertEqual(self.state.stats['mode_switch'], 1)
        await self.frame(.01, raw)
        await until(lambda: 'buy' in self.state.main)
        self.assertEqual((self.entry()['price'], self.entry()['quote_mode']), (D('99.95'), 'behind'))
        self.assertEqual(self.state.main['sell'], original['sell'])
        for _ in range(3):
            await self.frame(.1, raw)
        self.assertEqual(self.deletes(), 1)
        self.assertEqual(len(self.exchange.posts()), 3)

    async def test_boundary_oscillation_does_not_change_valid_same_level_order(self):
        original = dict(self.state.main)
        await self.start()
        for _ in range(4):
            await self.frame(.3, self.restore_book())
            await self.frame(.3)
        self.assertEqual(self.state.main, original)
        self.assertEqual(self.deletes(), 0)

    async def test_rest_confirmation_records_mode_change_and_preserves_selected_mode(self):
        await self.start()
        await self.frame(.1, self.restore_book())
        raw = push_book(self.exchange, self.bot, self.state, self.restore_book())
        self.clock[0] += 3.1
        self.bot.ws_alive_at = self.clock[0]
        with patch.object(self.bot, 'verify_quiet_book', return_value=raw):
            self.state.wake.set()
            await until(lambda: 'buy' not in self.state.main)
        self.assertEqual(self.state.stats['mode_switch'], 1)
        self.assertEqual(self.state.anchors['buy']['mode'], 'behind')
        await self.frame(.01, self.restore_book())
        await until(lambda: 'buy' in self.state.main)
        self.assertEqual(self.entry()['price'], D('99.95'))

    async def test_rest_thin_protection_cancels_with_protection_reason(self):
        raw = copy.deepcopy(RESTORABLE)
        raw['asks'][1][1] = '4'
        raw = push_book(self.exchange, self.bot, self.state, raw)
        self.clock[0] += 3.1
        self.bot.ws_alive_at = self.clock[0]
        disabled = app.logging.root.manager.disable
        app.logging.disable(app.logging.NOTSET)
        self.addCleanup(app.logging.disable, disabled)
        with patch.object(self.bot, 'verify_quiet_book', return_value=raw), self.assertLogs(app.log, level='INFO') as logs:
            await self.start()
            await until(lambda: 'sell' not in self.state.main)
        self.assertIn('buy', self.state.main)
        self.assertIn('reason=protection_thin', '\n'.join(logs.output))

    async def test_protection_loss_cancels_even_with_restore_confirmation_pending(self):
        raw = self.restore_book()
        await self.start()
        await self.frame(.1, raw)
        raw['bids'][1][1] = '4'
        await self.frame(.01, raw)
        await until(lambda: 'buy' not in self.state.main)
        self.assertEqual(self.state.stats['mode_switch'], 0)
        self.assertIn('sell', self.state.main)

    async def test_same_level_partial_fill_replenishes_full_order_after_cancel(self):
        before = self.entry()['size']
        self.bot.accept_fills(self.state, [self.exchange.fill(self.entry()['id'], '1')])
        await self.start()
        await until(lambda: 'buy' not in self.state.main)
        await self.bot.refresh_positions()
        await self.frame(.1)
        self.assertNotIn('buy', self.state.main)
        await self.frame(.51)
        await until(lambda: 'buy' in self.state.main)
        self.assertEqual(self.entry()['size'], before)
        self.assertEqual(self.entry()['quote_mode'], 'same_level')
        self.assertEqual(len(self.exchange.posts()), 3)

    async def test_risk_trip_cancels_both_same_level_orders(self):
        self.bot.tripped = 'risk'
        await self.start()
        await until(lambda: not self.state.main)
        self.assertEqual(len(self.exchange.posts()), 2)

    async def test_protection_loss_log_has_quantity_mode_and_threshold(self):
        disabled = app.logging.root.manager.disable
        app.logging.disable(app.logging.NOTSET)
        self.addCleanup(app.logging.disable, disabled)
        raw = copy.deepcopy(RESTORABLE)
        raw['bids'][1][1] = '4'
        with self.assertLogs(app.log, level='INFO') as logs:
            await self.start()
            await self.frame(.01, raw)
            await until(lambda: 'buy' not in self.state.main)
        text = '\n'.join(logs.output)
        for expected in ('reason=protection_thin', 'mode=same_level', 'external_qty=4', 'protection_min=5.002'):
            self.assertIn(expected, text)


if __name__ == '__main__':
    unittest.main()
