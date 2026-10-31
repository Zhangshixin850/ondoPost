import asyncio
import copy
import os
import contextlib
import io
import runpy
import sys
import time
import unittest
from decimal import Decimal as D
from unittest.mock import patch, Mock

import postOnly as app
from ondo_exchange import AccountLimiter, OndoAPIError, OndoExchange, RequestLimiter
from test_bot import FakeExchange, fixture, push_book, until


class ConfigTests(unittest.TestCase):
    def config(self, **settings):
        env = {'symbol1': 'TEST', 'orderSize1': '1000', 'myLimit1': '5000',
               'symbol2': '', 'symbol3': ''}
        env.update(settings)
        with patch.dict(os.environ, env, clear=True), patch.object(app, 'load_dotenv'):
            return app.Config.from_env()

    def test_per_market_sizes_and_disabled_slot(self):
        c = self.config(symbol3='sndk', orderSize3='250', myLimit3='1500', orderSize2='invalid')
        self.assertEqual(c.symbols, ('TEST', 'SNDK'))
        self.assertEqual([m.order_size for m in c.markets], [D(1000), D(250)])
        self.assertEqual(c.markets[1].position_limit, D(1500))
        self.assertEqual((c.mmr, c.max_leverage), (D('.2'), D(5)))

    def test_invalid_values_and_symbols(self):
        for settings in ({'symbol1': ''}, {'symbol2': 'test'}, {'orderSize1': ''},
                         {'orderSize1': 'NaN'}, {'myLimit1': '-1'}, {'MMR': '0'}, {'leverage': 'Infinity'}):
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                self.config(**settings)

    def test_check_cli_prints_per_market_settings_without_network(self):
        env={'symbol1':'CRCL','orderSize1':'250','myLimit1':'1500','symbol2':'','symbol3':''}
        output=io.StringIO()
        with patch.dict(os.environ,env,clear=True), patch('dotenv.load_dotenv'), \
             patch.object(sys,'argv',['postOnly.py','--check']), \
             patch.object(OndoExchange,'from_env',side_effect=AssertionError('Network forbidden')), \
             contextlib.redirect_stdout(output):
            runpy.run_path('postOnly.py',run_name='__main__')
        self.assertIn('orderSize=250',output.getvalue())
        self.assertIn('myLimit=1500',output.getvalue())
        self.assertIn('2/2 RPS',output.getvalue())


class WallTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.exchange, self.bot, self.state = fixture()
        self.addCleanup(self.exchange.close)

    async def test_largest_level_one_tick_behind_in_one_batch(self):
        await self.bot.place_quotes(self.state)
        self.assertEqual([(o['side'], D(o['price'])) for o in self.exchange.posts()],
                         [('buy', D('99.97')), ('sell', D('100.03'))])
        self.assertEqual(sum(m == 'POST' and p.endswith('/batch') for m,p,b in self.exchange.calls), 1)
        for o in self.exchange.posts():
            self.assertTrue(o['postOnly'])
            self.assertFalse(o['reduceOnly'])
            self.assertLessEqual(D(o['size']) * D(o['price']), D(1000))

    async def test_own_manual_and_main_orders_are_subtracted(self):
        manual = self.exchange.create_order('TEST', 'limit', 'buy', '100', '99.96', {'clientOrderId': 'manual'})
        self.bot.observe_order(self.state, manual['info'])
        push_book(self.exchange, self.bot, self.state)
        self.assertEqual(self.bot.quote_targets(self.state)['buy'], D('99.97'))
        await self.bot.place_quotes(self.state)
        push_book(self.exchange, self.bot, self.state)
        self.assertEqual(self.bot.quote_targets(self.state)['buy'], D('99.97'))

    def test_wall_uses_notional_and_tie_prefers_further_price(self):
        raw = {'bids': [['99.99', '1'], ['99.98', '99.96'], ['99.96', '99.98']],
               'asks': [['100.01', '1'], ['100.02', '1']]}
        push_book(self.exchange, self.bot, self.state, raw)
        self.assertEqual(self.bot.quote_targets(self.state)['buy'], D('99.95'))

    def test_outside_boundary_pauses_without_second_choice(self):
        raw = {'bids': [['99.99','1'], ['99.95','100']], 'asks': [['100.01','1'], ['100.04','100']]}
        push_book(self.exchange, self.bot, self.state, raw)
        self.assertIsNone(self.bot.quote_targets(self.state)['buy'])
        self.assertEqual(self.bot.quote_targets(self.state)['sell'], D('100.05'))

    def test_coarse_tick_no_valid_quote(self):
        raw = {'bids': [['5.22','2000']], 'asks': [['5.25','2000']]}
        push_book(self.exchange, self.bot, self.state, raw)
        self.assertEqual(self.bot.quote_targets(self.state), {'buy':None,'sell':None})

    def test_bad_self_deduction_pauses(self):
        self.state.own_orders['manual'] = {'side':'buy','price':'99.98','size':'100','filledSize':'0','status':'open'}
        with self.assertRaisesRegex(ValueError, '不同步'):
            self.bot.quote_targets(self.state)

    def test_snapshots_replace_reject_old_and_grouped(self):
        raw = push_book(self.exchange, self.bot, self.state)
        count = self.state.depth_seq
        self.bot.set_book(self.state, raw)
        self.assertEqual(self.state.depth_seq, count)
        raw['bids'] = [['99.99','1']]
        push_book(self.exchange, self.bot, self.state, raw)
        self.assertEqual(len(self.state.depth[0]), 1)
        raw['depthLevels'] = '0.1'
        with self.assertRaises(ValueError):
            push_book(self.exchange, self.bot, self.state, raw)

    def test_untriggered_manual_order_is_not_resting_liquidity(self):
        self.state.own_orders['stop']={'side':'buy','price':'0','size':'100','filledSize':'0',
                                       'status':'untriggered'}
        self.assertEqual(self.bot.quote_targets(self.state)['buy'],D('99.97'))

    async def test_stale_depth_cancels_and_no_new_quotes(self):
        await self.bot.place_quotes(self.state)
        self.state.depth_at -= 10
        worker = asyncio.create_task(self.bot.run_market(self.state))
        try:
            await until(lambda: not self.state.main)
            self.assertEqual(len(self.exchange.posts()), 2)
        finally:
            self.bot.request_stop()
            await worker

    async def test_partial_fill_only_replaces_filled_side_full_notional(self):
        await self.bot.place_quotes(self.state)
        sell = self.state.main['sell']
        fill = self.exchange.fill('1','2')
        self.bot.accept_fills(self.state,[fill,fill])
        self.assertEqual(self.state.tracked[self.state.main['buy']]['seen_size'],D(2))
        await self.bot.cancel_main(self.state,('buy',))
        self.assertNotIn('buy',self.state.main)
        self.assertEqual(self.state.main['sell'],sell)
        await self.bot.refresh_positions()
        push_book(self.exchange,self.bot,self.state)
        await self.bot.place_quotes(self.state)
        self.assertEqual(len(self.exchange.posts()),3)
        self.assertGreater(D(self.exchange.posts()[-1]['size'])*D(self.exchange.posts()[-1]['price']),D(999))
        self.assertTrue(all(o['postOnly'] for o in self.exchange.posts()))

    async def test_fill_during_cancel_is_accounted_and_waits_for_position(self):
        await self.bot.place_quotes(self.state)
        self.exchange.cancel_fill='1'
        await self.bot.cancel_main(self.state,('buy',))
        self.assertTrue(self.state.position_dirty)

        self.assertNotIn('buy',self.state.main)
        push_book(self.exchange,self.bot,self.state)
        await self.bot.place_quotes(self.state)
        self.assertEqual(len(self.exchange.posts()),2)

    async def test_unknown_batch_never_resubmitted(self):
        self.exchange.submit_mode='missing'
        await self.bot.place_quotes(self.state)
        self.assertTrue(self.state.fatal)
        count = len(self.exchange.posts())
        await self.bot.place_quotes(self.state)
        self.assertEqual(len(self.exchange.posts()),count)
        self.assertEqual(sum(m=='POST' and p.endswith('/batch') for m,p,b in self.exchange.calls),1)
        self.assertEqual(len(self.state.main),2)

    async def test_position_limit_latched(self):
        self.exchange.position_qty=D(51)
        await self.bot.refresh_positions()
        self.assertTrue(self.state.stopped)
        self.exchange.position_qty=D(0)
        await self.bot.refresh_positions()
        self.assertFalse(self.bot.quote_allowed(self.state))

    async def test_best_quote_is_canceled_while_waiting_for_depth(self):
        await self.bot.place_quotes(self.state)
        from datetime import datetime, timezone
        stamp=max(time.time(),self.state.book_time+.001)
        self.bot.set_book(self.state, {'time':datetime.fromtimestamp(stamp,timezone.utc).isoformat(),
                          'bids':[['99.97','10']], 'asks':[['100.01','1']]}, depth=False)
        worker=asyncio.create_task(self.bot.run_market(self.state))
        try:
            await until(lambda:'buy' not in self.state.main)
            self.assertIn('sell',self.state.main)
        finally:
            self.bot.request_stop()
            await worker

    async def test_pre_send_validation_rejects_changed_wall(self):
        clock=[0.0]
        def wait(delay):
            clock[0]+=delay
            push_book(self.exchange,self.bot,self.state,
                      {'bids':[['99.99','100']], 'asks':[['100.01','100']]})
        limiter=RequestLimiter(2,clock=lambda:clock[0],wait=wait)
        limiter.normal_rps=2
        self.bot.limiter.buckets['place']=limiter
        limiter.acquire()
        self.exchange._request=OndoExchange._request.__get__(self.exchange)
        self.exchange._session.send=Mock()
        await self.bot.place_quotes(self.state)
        self.exchange._session.send.assert_not_called()
        self.assertFalse(self.state.main)

    async def test_changed_wall_cancels_before_replacing(self):
        await self.bot.place_quotes(self.state)
        push_book(self.exchange,self.bot,self.state,
                  {'bids':[['99.99','1'],['99.97','100']], 'asks':[['100.01','1'],['100.02','20']]})
        worker=asyncio.create_task(self.bot.run_market(self.state))
        try:
            await until(lambda:'buy' not in self.state.main)
            self.assertEqual(len(self.exchange.posts()),2)
            push_book(self.exchange,self.bot,self.state,
                      {'bids':[['99.99','1'],['99.97','100']], 'asks':[['100.01','1'],['100.02','20']]})
            await until(lambda:'buy' in self.state.main)
            self.assertEqual(self.state.tracked[self.state.main['buy']]['price'],D('99.96'))
        finally:
            self.bot.request_stop()
            await worker

    async def test_same_wall_quantity_change_keeps_queue_position(self):
        await self.bot.place_quotes(self.state)
        push_book(self.exchange,self.bot,self.state,
                  {'bids':[['99.99','1'],['99.98','21']], 'asks':[['100.01','1'],['100.02','21']]})
        worker=asyncio.create_task(self.bot.run_market(self.state))
        try:
            await asyncio.sleep(.05)
            self.assertFalse(any(m=='DELETE' for m,p,b in self.exchange.calls))
            self.assertEqual(len(self.exchange.posts()),2)
        finally:
            self.bot.request_stop()
            await worker

    def test_stale_server_timestamp_is_not_freshened_by_receipt(self):
        from datetime import datetime, timezone
        before = self.state.depth_seq
        raw = copy.deepcopy(self.exchange.book)
        raw['time'] = datetime.fromtimestamp(time.time()-10, timezone.utc).isoformat()
        self.bot.set_book(self.state, raw)
        self.assertEqual(self.state.depth_seq, before)

    async def test_cancel_preserves_manual_and_legacy_orders(self):
        manual=self.exchange.create_order('TEST','limit','buy','1','99.90',{'clientOrderId':'manual'})
        legacy=self.exchange.create_order('TEST','limit','sell','1','100.10',{'clientOrderId':'po_h_old'})
        await self.bot.place_quotes(self.state)
        await self.bot.cancel_main(self.state)
        self.assertFalse(self.state.main)
        self.assertEqual(self.exchange.orders[manual['id']]['status'],'open')
        self.assertEqual(self.exchange.orders[legacy['id']]['status'],'open')

    async def test_partial_batch_failure_retains_success(self):
        original=self.exchange._request
        def request(method,path,params=None,body=None,private=True):
            if method=='POST' and path=='/v1/perps/orders' and body['side']=='sell':
                raise OndoAPIError('post only',code='post_only_has_match')
            return original(method,path,params,body,private)
        self.exchange._request=request
        await self.bot.place_quotes(self.state)
        self.assertEqual(set(self.state.main),{'buy'})
        self.assertFalse(self.state.fatal)
        self.assertEqual(len(self.state.tracked),1)

    async def test_timeout_after_entire_batch_acceptance_only_queries(self):
        original=self.exchange._request
        count=0
        def request(method,path,params=None,body=None,private=True):
            nonlocal count
            if method=='POST' and path.endswith('/batch'):
                count+=1
                original(method,path,params,body,private)
                raise OndoAPIError('timeout after acceptance',uncertain=True)
            return original(method,path,params,body,private)
        self.exchange._request=request
        await self.bot.place_quotes(self.state)
        self.assertEqual(count,1)
        self.assertEqual(len(self.state.main),2)
        self.assertFalse(self.state.fatal)

    async def test_inflight_batch_risk_trip_cleans_both_sides(self):
        self.exchange.on_create=lambda:setattr(self.bot,'tripped','risk breach')
        await self.bot.place_quotes(self.state)
        self.assertFalse(self.state.main)
        self.assertTrue(all(o['status']=='canceled' for o in self.exchange.orders.values()))

    async def test_new_quote_amount_uses_market_setting(self):
        self.state.order_size=D(250)
        await self.bot.place_quotes(self.state)
        self.assertTrue(all(D(249)<D(o['size'])*D(o['price'])<=D(250)
                            for o in self.exchange.posts()))

    async def test_cancel_failure_keeps_slot_and_blocks_replacement(self):
        await self.bot.place_quotes(self.state)
        with patch.object(self.exchange,'cancel_orders',side_effect=OndoAPIError('timeout',uncertain=True)):
            await self.bot.cancel_main(self.state)
        self.assertEqual(len(self.state.main),2)
        self.assertTrue(self.state.problem)
        push_book(self.exchange,self.bot,self.state)
        await self.bot.place_quotes(self.state)
        self.assertEqual(len(self.exchange.posts()),2)

    async def test_reconnect_requires_new_generation_and_depth(self):
        self.state.synced_generation=-1
        await self.bot.place_quotes(self.state)
        self.assertFalse(self.exchange.posts())
        self.state.synced_generation=self.bot.generation
        self.state.depth_at=0
        await self.bot.place_quotes(self.state)
        self.assertFalse(self.exchange.posts())

    async def test_startup_refuses_legacy_main_in_unconfigured_market(self):
        orders=[{'symbol':'OTHER', 'clientOrderId':'po_q_old'}]
        with patch.object(self.exchange,'fetch_open_orders',return_value=orders):
            with self.assertRaisesRegex(RuntimeError,'遗留主单'):
                await self.bot.run()
        self.assertFalse(self.exchange.posts())

    async def test_disconnect_cancels_existing_orders(self):
        await self.bot.place_quotes(self.state)
        self.bot.ws_ready=False
        worker=asyncio.create_task(self.bot.run_market(self.state))
        try:
            await until(lambda:not self.state.main)
        finally:
            self.bot.request_stop()
            await worker

    async def test_main_sync_waits_for_all_fill_history(self):
        await self.bot.place_quotes(self.state)
        entry=self.state.tracked[self.state.main['buy']]
        self.exchange.fill(entry['id'],'1')
        self.exchange.fill_lag=1
        self.bot.observe_order(self.state,self.exchange.orders[entry['id']])
        with self.assertRaisesRegex(OndoAPIError,'尚未齐全'):
            await self.bot.sync_main(self.state,entry)
        self.assertIn('buy',self.state.main)
        await self.bot.cancel_main(self.state,('buy',))
        self.assertNotIn('buy',self.state.main)
        self.assertTrue(self.state.position_dirty)

class MultiMarketTests(unittest.IsolatedAsyncioTestCase):
    async def test_shared_limits_and_global_stop_cancel_all_three_markets(self):
        clients={}
        metadata={}
        for name in ('AAA','BBB','CCC'):
            client=FakeExchange()
            self.addCleanup(client.close)
            market={'id':name+'-USD.P','symbol':name+'/USDT:USDT','base':name,
                    'precision':{'amount':'0.001','price':'0.01'}}
            clients[market['symbol']]=client
            metadata[market['symbol']]=market
        for client in clients.values():
            client._markets=metadata
            client._markets_by_id={m['id']:m for m in metadata.values()}
        risk=next(iter(clients.values()))
        config=app.Config(tuple(app.MarketConfig(n,D(1000),D(5000)) for n in ('AAA','BBB','CCC')))
        bot=app.PostOnlyBot(config,risk,clients)
        bot.risk_ok=bot.ws_ready=True
        for state in bot.states.values():
            state.synced_generation=bot.generation
            state.position_dirty=False
            push_book(clients[state.symbol],bot,state)
            self.assertIs(clients[state.symbol].limiter,bot.limiter)
            await bot.place_quotes(state)
        self.assertEqual(sum(len(c.orders) for c in clients.values()),6)
        bot.tripped='global leverage breach'
        workers=[asyncio.create_task(bot.run_market(s)) for s in bot.states.values()]
        try:
            await until(lambda:all(not s.main for s in bot.states.values()))
            self.assertTrue(all(o['status']=='canceled' for c in clients.values() for o in c.orders.values()))
        finally:
            bot.request_stop()
            await asyncio.gather(*workers)

