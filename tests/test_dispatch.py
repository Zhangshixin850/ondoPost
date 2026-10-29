import asyncio
import contextlib
import copy
import io
import os
import time
import unittest
from decimal import Decimal as D
from unittest.mock import Mock, patch

import postOnly as app
from ondo_exchange import OndoAPIError, OndoExchange, RequestLimiter
from test_bot import FakeExchange, fixture, until


class ConfigTests(unittest.TestCase):
    def config(self, **settings):
        env = {"symbol1": "CRCL", "symbol2": "SNDK", "symbol3": "",
               "orderSize": "1000", "myLimit": "5000",
               "nearLimit": "2", "farLimit": "4.9", "rps": "5"}
        env.update(settings)
        with patch.dict(os.environ, env, clear=True), patch.object(app, "load_dotenv"):
            return app.Config.from_env()

    def test_empty_slots_are_disabled(self):
        self.assertEqual(self.config().symbols, ("CRCL", "SNDK"))
        self.assertEqual(self.config(symbol1="", symbol2="sndk").symbols, ("SNDK",))
        self.assertEqual(self.config(symbol1="", symbol2="", symbol3="soxl").symbols, ("SOXL",))

    def test_all_empty_or_duplicate_symbols_are_rejected(self):
        for settings in (dict(symbol1="", symbol2=""), dict(symbol2="crcl")):
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                self.config(**settings)

    def test_rps_and_band_validation(self):
        for rps in ("1", "5", "10"):
            self.assertEqual(self.config(rps=rps).rps, int(rps))
        for rps in ("0", "-1", "1.5", "NaN", "", "abc"):
            with self.subTest(rps=rps), self.assertRaises(ValueError):
                self.config(rps=rps)
        for settings in (
            dict(nearLimit="0"), dict(nearLimit="5"), dict(farLimit="3.9"),
            dict(nearLimit="4", farLimit="4"), dict(farLimit="NaN"),
        ):
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                self.config(**settings)


class DispatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.exchange, self.bot, self.state = fixture()
        self.addCleanup(self.exchange.close)

    async def test_startup_preflight_retries_only_rate_limited_read(self):
        calls = 0
        def orders():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OndoAPIError("rate limited", status=429, retry_after=2)
            return [{"symbol": "OTHER/USDT:USDT", "clientOrderId": "po_q_old"}]
        with patch.object(self.exchange, "fetch_open_orders", side_effect=orders):
            with self.assertRaisesRegex(RuntimeError, "遗留主单"):
                await self.bot.run()
        self.assertEqual(calls, 2)
        self.assertFalse(self.exchange.posts())

    async def test_market_initialization_retries_429_without_writing_orders(self):
        config = app.Config(("TEST", "TEST-USD.P"), D("1000"), D("5000"),
                            D("3"), D(".2"), D("5"))
        attempts = [OndoAPIError("rate limited", status=429), self.exchange._markets]
        with patch.object(app.OndoExchange, "from_env", return_value=self.exchange), \
                patch.object(self.exchange, "load_markets", side_effect=attempts) as load:
            with self.assertRaisesRegex(ValueError, "相同市场"):
                await app.main(config)
        self.assertEqual(load.call_count, 2)
        self.assertFalse(self.exchange.posts())

    async def test_queued_quote_is_dropped_if_price_or_risk_changes(self):
        for change in ("price", "risk"):
            with self.subTest(change=change):
                exchange, bot, state = fixture()
                try:
                    clock = [0.0]
                    def wait(seconds):
                        clock[0] += seconds
                        if change == "price":
                            bot.set_book(state, {"bids": [["100.09", "1"]],
                                                 "asks": [["100.11", "1"]]})
                        else:
                            bot.tripped = "risk tripped while queued"
                    limiter = RequestLimiter(1, clock=lambda: clock[0], wait=wait)
                    bot.limiter = exchange.limiter = limiter
                    limiter.acquire(priority=0)
                    exchange._request = OndoExchange._request.__get__(exchange)
                    exchange._session.send = Mock()
                    await bot.place_quote(state, "buy")
                    exchange._session.send.assert_not_called()
                    self.assertFalse(state.main)
                    self.assertFalse(state.tracked)
                finally:
                    exchange.close()

    async def test_position_snapshot_is_not_incremented_again_by_fill(self):
        await self.bot.place_quote(self.state, "buy")
        fill = self.exchange.fill("1", "1")
        self.state.position = D("8")
        self.state.position_seq = 10
        self.state.position_dirty = False
        fill["_position_seq"] = 9
        self.bot.accept_fills(self.state, [fill])
        self.assertEqual(self.state.position, D("8"))
        self.assertFalse(self.state.position_dirty)
        self.assertEqual(len(self.state.hedges), 1)

    async def test_configured_band_keeps_order_between_two_and_three_bps(self):
        await self.bot.place_quote(self.state, "buy")
        self.bot.set_book(self.state, {"bids": [["99.975", "1"]],
                                       "asks": [["99.995", "1"]]})
        worker = asyncio.create_task(self.bot.run_market(self.state))
        try:
            await asyncio.sleep(.06)
            self.assertEqual(self.exchange.orders["1"]["status"], "open")
            self.assertFalse(any(m == "DELETE" for m, p, b in self.exchange.calls))
        finally:
            self.bot.request_stop()
            await asyncio.wait_for(worker, 3)

    async def test_high_frequency_positions_do_not_trigger_per_message_rest(self):
        completed = asyncio.Event()

        async def stream(markets):
            yield {"type": "ready"}
            for _ in range(300):
                yield {"type": "update", "channel": "positionsPerps", "data": [{
                    "market": "TEST-USD.P", "netQuantity": "0",
                    "direction": "neutral", "markPrice": "100",
                }]}
                yield {"type": "update", "channel": "topOfBooksPerps",
                       "data": [{"market": "TEST-USD.P", **self.exchange.book}]}
                await asyncio.sleep(0)
            completed.set()
            await self.bot.stop_event.wait()

        self.exchange.stream = stream
        self.bot.ws_ready = False
        self.state.synced_generation = -1
        self.state.position_dirty = True
        tasks = [asyncio.create_task(self.bot.feed_loop()),
                 asyncio.create_task(self.bot.account_loop()),
                 asyncio.create_task(self.bot.run_market(self.state))]
        try:
            await asyncio.wait_for(completed.wait(), 3)
            await until(lambda: len(self.state.main) == 2)
            await asyncio.sleep(.1)
            self.assertLessEqual(sum(p == "/v1/perps/positions" for m, p, b in self.exchange.calls), 2)
            self.assertLessEqual(sum(p == "/v1/perps/orders" and m == "GET"
                                     for m, p, b in self.exchange.calls), 2)
            self.assertEqual(len(self.exchange.posts()), 2)
        finally:
            self.bot.request_stop()
            tasks[0].cancel()
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 4)

    async def test_cooldown_is_not_broken_by_market_wakeups(self):
        await self.bot.place_quote(self.state, "buy")
        fill = self.exchange.fill("1", "1")
        self.bot.accept_fills(self.state, [fill])
        clock = [0.0]
        limiter = RequestLimiter(5, clock=lambda: clock[0])
        self.bot.limiter = self.exchange.limiter = limiter
        limiter.defer(2)
        self.bot.recovering = True
        self.exchange.calls.clear()
        worker = asyncio.create_task(self.bot.run_market(self.state))
        try:
            for _ in range(100):
                self.state.wake.set()
                await asyncio.sleep(0)
            self.assertFalse(self.exchange.calls)
            self.assertEqual(len(self.state.hedges), 1)
        finally:
            clock[0] = 3
            self.bot.request_stop()
            await asyncio.wait_for(worker, 4)

    async def test_rate_limited_order_lookup_never_resubmits_hedge(self):
        await self.bot.place_quote(self.state, "buy")
        self.bot.accept_fills(self.state, [self.exchange.fill("1", "1")])
        original = self.exchange._request
        rejected = False

        def request(method, path, **kwargs):
            nonlocal rejected
            if method == "GET" and "/client:" in path and not rejected:
                rejected = True
                raise OndoAPIError("rate limited", code="too_many_requests", status=429, retry_after=2)
            return original(method, path, **kwargs)

        self.exchange.submit_mode = "accepted"
        with patch.object(self.exchange, "_request", side_effect=request):
            await self.bot.send_hedges(self.state)
        hedge = next(iter(self.state.hedges.values()))
        self.assertTrue(hedge["unknown"])
        self.assertFalse(self.state.fatal)
        count = len(self.exchange.posts())
        self.bot.limiter.cooldown_until = 0
        hedge["retry_at"] = 0
        await self.bot.send_hedges(self.state)
        self.assertFalse(self.state.hedges)
        self.assertEqual(len(self.exchange.posts()), count)

    async def test_initial_preflight_detects_main_orders_in_disabled_market(self):
        market = copy.deepcopy(self.exchange.market("TEST"))
        market.update(id="OTHER-USD.P", symbol="OTHER/USDT:USDT", base="OTHER")
        self.exchange._markets[market["symbol"]] = market
        self.exchange._markets_by_id[market["id"]] = market
        self.exchange.orders["old"] = {
            "orderId": "old", "clientOrderId": "po_q_old_run",
            "market": "OTHER-USD.P", "price": "100", "size": "1",
            "filledSize": "0", "side": "buy", "status": "open",
        }
        with self.assertRaisesRegex(RuntimeError, "未启用市场"):
            await self.bot.run()

    async def test_account_reconciliation_is_shared_across_markets(self):
        clients = {}
        for base in ("AAA", "BBB"):
            exchange = FakeExchange()
            self.addCleanup(exchange.close)
            market = copy.deepcopy(exchange.market("TEST"))
            market.update(id=base + "-USD.P", symbol=base + "/USDT:USDT", base=base)
            exchange._markets = {market["symbol"]: market}
            exchange._markets_by_id = {market["id"]: market}
            clients[market["symbol"]] = exchange
        account = FakeExchange()
        self.addCleanup(account.close)
        account._markets = {k: c.market(k) for k, c in clients.items()}
        account._markets_by_id = {m["id"]: m for m in account._markets.values()}
        bot = app.PostOnlyBot(self.bot.config, account, clients, account)
        bot.ws_ready = True
        await bot.reconcile()
        self.assertEqual(sum(p == "/v1/perps/positions" for m, p, b in account.calls), 1)
        self.assertEqual(sum(p == "/v1/perps/orders" for m, p, b in account.calls), 1)
        self.assertFalse(any(client.calls for client in clients.values()))

    async def test_old_rest_snapshot_does_not_overwrite_newer_position_push(self):
        original = self.exchange.fetch_positions

        def snapshot(*args):
            self.state.position = D("7")
            self.state.position_seq += 1
            self.state.position_dirty = False
            return original(*args)

        with patch.object(self.exchange, "fetch_positions", side_effect=snapshot):
            await self.bot.refresh_positions()
        self.assertEqual(self.state.position, D("7"))

    async def test_risk_display_continues_during_cooldown(self):
        self.bot.last_risk = {"marginRatio": D(".1"), "leverage": D("2")}
        self.bot.risk_at = time.monotonic()
        self.bot.recovering = True
        self.bot.limiter.defer(2)
        output = io.StringIO()
        with patch.object(app, "RISK_SECONDS", .02), contextlib.redirect_stdout(output):
            task = asyncio.create_task(self.bot.risk_loop())
            await asyncio.sleep(.08)
            self.bot.request_stop()
            await task
        self.assertIn("冷却/恢复中", output.getvalue())
        self.assertIn("数据年龄=", output.getvalue())
        self.assertFalse(any(p == "/v1/perps/balance" for m, p, b in self.exchange.calls))


if __name__ == "__main__":
    unittest.main()
