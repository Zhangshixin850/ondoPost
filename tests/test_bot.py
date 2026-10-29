import asyncio
import contextlib
import copy
import hashlib
import hmac
import io
import json
import logging
import os
import time
import unittest
from decimal import Decimal as D
from unittest.mock import patch

import ondo_exchange as adapter
import postOnly as app

logging.disable(logging.CRITICAL)


class FakeExchange(adapter.OndoExchange):
    def __init__(self):
        super().__init__("test_key", "test_secret")
        market = {
            "id": "TEST-USD.P", "symbol": "TEST/USDT:USDT", "base": "TEST",
            "precision": {"amount": "0.001", "price": "0.01"},
        }
        self._markets = {market["symbol"]: market}
        self._markets_by_id = {market["id"]: market}
        self.orders = {}
        self.fills = {}
        self.calls = []
        self.risk = {"marginRatio": "0.1", "leverage": "2", "marginBalance": "1000"}
        self.position_qty = D("0")
        self.book = {"bids": [["99.99", "100"]], "asks": [["100.01", "100"]]}
        self.submit_mode = ""
        self.reject_hedges = False
        self.risk_error = False
        self.cancel_fill = None
        self.fill_lag = 0
        self.on_create = None

    def fill(self, order_id, qty, price=None, fill_id=None):
        raw = self.orders[order_id]
        qty = D(qty)
        item = {
            "id": fill_id or f"f{len(self.fills[order_id])}_{order_id}",
            "orderId": order_id, "clientOrderId": raw["clientOrderId"],
            "market": raw["market"], "side": raw["side"],
            "price": str(price or raw["price"]), "size": str(qty),
        }
        self.fills[order_id].append(item)
        raw["filledSize"] = str(D(raw["filledSize"]) + qty)
        raw["status"] = "fullyfilled" if D(raw["filledSize"]) == D(raw["size"]) else "open"
        self.position_qty += qty if raw["side"] == "buy" else -qty
        return copy.deepcopy(item)

    def _request(self, method, path, params=None, body=None, private=True):
        self.calls.append((method, path, copy.deepcopy(body)))
        if path == "/v1/perps/balance":
            if self.risk_error:
                raise adapter.OndoAPIError("risk unavailable")
            return {"result": copy.deepcopy(self.risk)}
        if path == "/v1/perps/positions":
            qty = self.position_qty
            positions = [] if not qty else [{
                "market": "TEST-USD.P", "netQuantity": str(abs(qty)),
                "direction": "long" if qty > 0 else "short",
                "unrealizedPnl": "0", "markPrice": "100",
            }]
            return {"result": positions}
        if path == "/v1/perps/depth":
            return {"result": copy.deepcopy(self.book)}
        if path == "/v1/perps/orders" and method == "GET":
            return {"result": [copy.deepcopy(o) for o in self.orders.values()
                               if o["status"] == "open"]}
        if path == "/v1/perps/orders" and method == "POST":
            if self.reject_hedges and not body["postOnly"]:
                raise adapter.OndoAPIError("insufficient funds", code="insufficient_funds")
            if self.submit_mode == "reject":
                raise adapter.OndoAPIError("post only", code="post_only_has_match")
            if self.submit_mode == "missing":
                raise adapter.OndoAPIError("timeout", uncertain=True)
            if any(o["clientOrderId"] == body["clientOrderId"] for o in self.orders.values()):
                raise adapter.OndoAPIError("collision", code="clientOrderID_collision")
            oid = str(len(self.orders) + 1)
            raw = {
                "orderId": oid, "clientOrderId": body["clientOrderId"],
                "market": body["market"], "side": body["side"],
                "price": body["price"], "size": body["size"],
                "filledSize": "0", "status": "open",
            }
            self.orders[oid] = raw
            self.fills[oid] = []
            if self.on_create:
                self.on_create()
            if self.submit_mode == "accepted":
                raise adapter.OndoAPIError("timeout after acceptance", uncertain=True)
            return {"result": copy.deepcopy(raw)}
        prefix = "/v1/perps/orders/"
        if path.startswith(prefix):
            identifier = path[len(prefix):]
            fills_request = identifier.endswith("/fills")
            if fills_request:
                identifier = identifier[:-6]
            if identifier.startswith("client:"):
                raw = next((o for o in self.orders.values()
                            if o["clientOrderId"] == identifier[7:]), None)
            else:
                raw = self.orders.get(identifier)
            if raw is None:
                raise adapter.OndoAPIError("missing", code="order_not_found")
            oid = raw["orderId"]
            if fills_request:
                if self.fill_lag:
                    self.fill_lag -= 1
                    return {"result": []}
                return {"result": copy.deepcopy(self.fills[oid])}
            if method == "DELETE":
                if raw["status"] == "fullyfilled":
                    raise adapter.OndoAPIError("already filled", code="order_already_filled")
                if self.cancel_fill:
                    qty = self.cancel_fill
                    self.cancel_fill = None
                    self.fill(oid, qty)
                if raw["status"] != "fullyfilled":
                    raw["status"] = "canceled"
            return {"result": copy.deepcopy(raw)}
        raise AssertionError((method, path))

    def posts(self):
        return [call[2] for call in self.calls
                if call[:2] == ("POST", "/v1/perps/orders")]


def fixture(verse="2"):
    exchange = FakeExchange()
    config = app.Config(("TEST", "SECOND", "THIRD"), D("1000"), D("5000"),
                        D(verse), D(".2"), D("5"))
    bot = app.PostOnlyBot(config, exchange, {"TEST/USDT:USDT": exchange})
    state = next(iter(bot.states.values()))
    bot.risk_ok = bot.ws_ready = True
    bot.generation = state.synced_generation = 1
    bot.set_book(state, exchange.book)
    state.position_dirty = False
    return exchange, bot, state


async def until(predicate, timeout=3):
    end = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= end:
            raise AssertionError("condition timed out")
        await asyncio.sleep(.005)


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.exchange = FakeExchange()
        self.addCleanup(self.exchange.close)

    def test_bare_symbol_alias(self):
        self.assertEqual(self.exchange.market("test")["id"], "TEST-USD.P")

    def test_post_only_and_explicit_client_id(self):
        order = self.exchange.create_order("TEST", "limit", "buy", "1", "99.96",
                                          {"postOnly": True, "clientOrderId": "po_q_test"})
        self.assertEqual(order["clientOrderId"], "po_q_test")
        self.assertTrue(self.exchange.posts()[0]["postOnly"])

    def test_partial_canceled_order_is_valid(self):
        order = self.exchange.create_order("TEST", "limit", "buy", "10", "99.96")
        self.exchange.fill(order["id"], "2")
        canceled = self.exchange.cancel_order(order["id"], "TEST")
        self.assertEqual(canceled["status"], "canceled")
        self.assertEqual(D(canceled["info"]["filledSize"]), D("2"))

    def test_timeout_after_acceptance_queries_without_second_post(self):
        self.exchange.submit_mode = "accepted"
        order = self.exchange.create_order("TEST", "limit", "buy", "1", "99.96")
        self.assertEqual(order["id"], "1")
        self.assertEqual(len(self.exchange.posts()), 1)

    def test_unconfirmed_post_is_never_repeated(self):
        self.exchange.submit_mode = "missing"
        with patch.object(adapter.time, "sleep"), self.assertRaises(adapter.OndoFatalError):
            self.exchange.create_order("TEST", "limit", "buy", "1", "99.96")
        self.assertEqual(len(self.exchange.posts()), 1)

    def test_client_id_collision_reconciles_matching_order(self):
        params = {"clientOrderId": "same_id", "postOnly": True}
        first = self.exchange.create_order("TEST", "limit", "buy", "1", "99.96", params)
        second = self.exchange.create_order("TEST", "limit", "buy", "1", "99.96", params)
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(len(self.exchange.orders), 1)

    def test_wrong_order_for_same_client_id_is_fatal(self):
        params = {"clientOrderId": "same_id"}
        self.exchange.create_order("TEST", "limit", "buy", "1", "99.96", params)
        with patch.object(adapter.time, "sleep"), self.assertRaises(adapter.OndoFatalError):
            self.exchange.create_order("TEST", "limit", "buy", "2", "99.96", params)

    def test_cancel_already_filled_still_reports_fill(self):
        order = self.exchange.create_order("TEST", "limit", "sell", "1", "100.04")
        self.exchange.fill(order["id"], "1")
        result = self.exchange.cancel_order(order["id"], "TEST")
        self.assertEqual(result["status"], "filled")
        self.assertEqual(len(self.exchange.fetch_order_fills(order["id"], "TEST")), 1)

    def test_risk_fields_are_decimal_and_not_filtered_by_symbol(self):
        self.exchange.risk["leverage"] = "6.2"
        risk = self.exchange.fetch_account_risk()
        self.assertEqual(risk["leverage"], D("6.2"))
        self.assertEqual(self.exchange.calls[-1][1], "/v1/perps/balance")

    def test_nonfinite_risk_is_rejected(self):
        for value in ("NaN", "Infinity", "-1"):
            with self.subTest(value=value):
                self.exchange.risk["marginRatio"] = value
                with self.assertRaises(ValueError):
                    self.exchange.fetch_account_risk()

    def test_position_retains_mark_and_signed_quantity(self):
        self.exchange.position_qty = D("-2")
        position = self.exchange.fetch_positions(["TEST"])[0]
        self.assertEqual(position["info"]["positionAmt"], "-2")
        self.assertEqual(position["info"]["markPrice"], "100")


class BotTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.exchange, self.bot, self.state = fixture()
        self.addCleanup(self.exchange.close)

    async def test_quotes_at_four_bps_and_within_notional(self):
        for side, price in (("buy", "99.96"), ("sell", "100.04")):
            await self.bot.place_quote(self.state, side)
            body = self.exchange.posts()[-1]
            self.assertEqual(D(body["price"]), D(price))
            self.assertLessEqual(D(body["size"]) * D(price), D("1000"))

    async def test_coarse_tick_skips_both_sides(self):
        self.bot.set_book(self.state, {"bids": [["25.00", "1"]], "asks": [["25.01", "1"]]})
        await self.bot.place_quote(self.state, "buy")
        await self.bot.place_quote(self.state, "sell")
        self.assertFalse(self.exchange.posts())

    async def test_verse_and_fill_deduplication(self):
        self.bot.config.verse = D("7")
        await self.bot.place_quote(self.state, "buy")
        fill = self.exchange.fill("1", "2", "100", "unique_fill")
        self.bot.accept_fills(self.state, [fill, fill])
        self.assertEqual(len(self.state.hedges), 1)
        await self.bot.send_hedges(self.state)
        hedge = self.exchange.posts()[-1]
        self.assertEqual(hedge["side"], "sell")
        self.assertEqual(D(hedge["price"]), D("100.07"))
        self.assertEqual(D(hedge["size"]), D("2"))
        self.assertFalse(hedge["postOnly"])

    async def test_sell_fill_creates_buy_at_verse(self):
        await self.bot.place_quote(self.state, "sell")
        fill = self.exchange.fill("1", "1", "100")
        self.bot.accept_fills(self.state, [fill])
        await self.bot.send_hedges(self.state)
        self.assertEqual(D(self.exchange.posts()[-1]["price"]), D("99.98"))

    async def test_hedge_rounds_away_from_fill_price(self):
        await self.bot.place_quote(self.state, "buy")
        fill = self.exchange.fill("1", "1", "100.001")
        self.bot.accept_fills(self.state, [fill])
        self.assertEqual(next(iter(self.state.hedges.values()))["price"], D("100.03"))

    async def test_cancel_race_hedges_every_fill_and_replenishes_full_size(self):
        await self.bot.place_quote(self.state, "buy")
        fill = self.exchange.fill("1", "2")
        self.bot.accept_fills(self.state, [fill])
        await self.bot.send_hedges(self.state)
        self.exchange.cancel_fill = "1"
        await self.bot.cancel_main(self.state, ("buy",))
        await self.bot.send_hedges(self.state)
        hedges = [p for p in self.exchange.posts() if not p["postOnly"]]
        self.assertEqual(sum(D(p["size"]) for p in hedges), D("3"))
        self.assertEqual(len(hedges), 2)
        await self.bot.reconcile(self.state)
        await self.bot.place_quote(self.state, "buy")
        new_quote = self.exchange.posts()[-1]
        self.assertTrue(new_quote["postOnly"])
        self.assertGreater(D(new_quote["size"]) * D(new_quote["price"]), D("999"))

    async def test_lagging_fill_history_does_not_release_order_slot(self):
        await self.bot.place_quote(self.state, "buy")
        self.exchange.fill("1", "1")
        self.exchange.fill_lag = 1
        await self.bot.cancel_main(self.state, ("buy",))
        self.assertIn("buy", self.state.main)
        self.assertTrue(self.state.problem)
        self.state.tracked[self.state.main['buy']]['retry_at'] = 0
        await self.bot.cancel_main(self.state, ("buy",))
        self.assertNotIn("buy", self.state.main)
        self.assertEqual(len(self.state.hedges), 1)

    async def test_hedge_fill_does_not_generate_another_hedge(self):
        await self.bot.place_quote(self.state, "buy")
        fill = self.exchange.fill("1", "1")
        self.bot.accept_fills(self.state, [fill])
        await self.bot.send_hedges(self.state)
        hedge_fill = self.exchange.fill("2", "1")
        self.bot.accept_fills(self.state, [hedge_fill])
        self.assertFalse(self.state.hedges)

    async def test_global_stop_cancels_only_main_orders(self):
        await self.bot.place_quote(self.state, "buy")
        fill = self.exchange.fill("1", "1")
        self.bot.accept_fills(self.state, [fill])
        await self.bot.send_hedges(self.state)
        self.bot.tripped = "risk limit"
        await self.bot.cancel_main(self.state)
        await self.bot.send_hedges(self.state)
        self.assertEqual(self.exchange.orders["1"]["status"], "canceled")
        self.assertEqual(self.exchange.orders["2"]["status"], "open")
        deleted = [p for m, p, b in self.exchange.calls if m == "DELETE"]
        self.assertEqual(deleted, ["/v1/perps/orders/1"])

    async def test_primary_accepted_during_risk_trip_is_canceled(self):
        self.exchange.on_create = lambda: setattr(self.bot, "tripped", "risk trip in flight")
        await self.bot.place_quote(self.state, "buy")
        self.assertFalse(self.state.main)
        self.assertEqual(self.exchange.orders["1"]["status"], "canceled")

    async def test_unknown_primary_keeps_client_id_for_cleanup(self):
        self.exchange.submit_mode = "missing"
        with patch.object(adapter.time, "sleep"):
            await self.bot.place_quote(self.state, "buy")
        self.assertTrue(self.state.fatal)
        self.assertIn("buy", self.state.main)
        self.assertEqual(len(self.exchange.posts()), 1)
        await self.bot.cancel_main(self.state)
        self.assertIn("buy", self.state.main)

    async def test_unknown_hedge_is_queried_without_resubmission(self):
        await self.bot.place_quote(self.state, "buy")
        fill = self.exchange.fill("1", "1")
        self.bot.accept_fills(self.state, [fill])
        self.exchange.submit_mode = "missing"
        with patch.object(adapter.time, "sleep"):
            await self.bot.send_hedges(self.state)
        hedge = next(iter(self.state.hedges.values()))
        self.assertTrue(hedge["unknown"])
        hedge["retry_at"] = 0
        count = len(self.exchange.posts())
        await self.bot.send_hedges(self.state)
        self.assertEqual(len(self.exchange.posts()), count)

    async def test_failed_hedge_pauses_primary_quotes(self):
        await self.bot.place_quote(self.state, "buy")
        fill = self.exchange.fill("1", "1")
        self.bot.accept_fills(self.state, [fill])
        self.exchange.reject_hedges = True
        await self.bot.send_hedges(self.state)
        self.assertTrue(self.bot.primary_paused(self.state))
        self.assertFalse(self.bot.quote_allowed(self.state))

    async def test_position_limit_is_latched(self):
        self.exchange.position_qty = D("51")
        await self.bot.reconcile(self.state)
        self.assertTrue(self.state.stopped)
        self.exchange.position_qty = D("0")
        await self.bot.reconcile(self.state)
        self.assertFalse(self.bot.quote_allowed(self.state))

    async def test_stale_primary_blocks_startup(self):
        await self.bot.place_quote(self.state, "buy")
        with self.assertRaisesRegex(RuntimeError, "遗留主单"):
            await self.bot.run()

    async def test_reprice_worker_and_graceful_shutdown(self):
        worker = asyncio.create_task(self.bot.run_market(self.state))
        try:
            await until(lambda: len(self.state.main) == 2)
            old_ids = set(self.exchange.orders)
            self.exchange.book = {"bids": [["100.01", "10"]], "asks": [["100.03", "10"]]}
            self.bot.set_book(self.state, self.exchange.book)
            self.state.wake.set()
            await until(lambda: len(self.exchange.orders) >= 4)
            for oid in old_ids:
                self.assertEqual(self.exchange.orders[oid]["status"], "canceled")
        finally:
            self.bot.request_stop()
            await asyncio.wait_for(worker, 4)
        self.assertFalse(self.state.main)

    async def test_disconnect_cancels_quotes_and_hedges_cancel_race(self):
        worker = asyncio.create_task(self.bot.run_market(self.state))
        try:
            await until(lambda: len(self.state.main) == 2)
            self.exchange.cancel_fill = "1"
            self.bot.ws_ready = False
            self.state.wake.set()
            await until(lambda: not self.state.main and
                        any(not p["postOnly"] for p in self.exchange.posts()))
            self.assertTrue(all(o["status"] != "open" for o in self.exchange.orders.values()
                                if o["clientOrderId"].startswith(app.MAIN_PREFIX)))
        finally:
            self.bot.request_stop()
            await asyncio.wait_for(worker, 4)

    async def test_risk_query_failure_removes_main_quotes(self):
        worker = asyncio.create_task(self.bot.run_market(self.state))
        risk_task = None
        try:
            await until(lambda: len(self.state.main) == 2)
            self.exchange.risk_error = True
            with contextlib.redirect_stdout(io.StringIO()):
                risk_task = asyncio.create_task(self.bot.risk_loop())
                await until(lambda: not self.bot.risk_ok and not self.state.main)
        finally:
            self.bot.request_stop()
            await asyncio.wait_for(worker, 4)
            if risk_task:
                await asyncio.wait_for(risk_task, 4)


class RiskTests(unittest.IsolatedAsyncioTestCase):
    async def check_risk(self, ratio, leverage, expected):
        exchange, bot, state = fixture()
        exchange.risk.update(marginRatio=ratio, leverage=leverage)
        bot.risk_ok = False
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            task = asyncio.create_task(bot.risk_loop())
            await until(lambda: bot.risk_ok)
            bot.request_stop()
            await task
        exchange.close()
        self.assertEqual(bool(bot.tripped), expected)
        self.assertIn("保证金率=", output.getvalue())
        self.assertIn("总杠杆=", output.getvalue())
        if expected:
            self.assertFalse(bot.quote_allowed(state))

    async def test_equal_thresholds_do_not_trip(self):
        await self.check_risk(".2", "5", False)

    async def test_margin_ratio_alone_trips(self):
        await self.check_risk(".200001", "2", True)

    async def test_leverage_alone_trips(self):
        await self.check_risk(".1", "5.000001", True)

    async def test_both_thresholds_trip(self):
        await self.check_risk(".3", "6", True)

    async def test_stop_remains_latched_after_metrics_recover(self):
        exchange, bot, state = fixture()
        bot.tripped = "previous breach"
        exchange.risk.update(marginRatio=".01", leverage="1")
        task = asyncio.create_task(bot.risk_loop())
        await until(lambda: any(p == "/v1/perps/balance" for m, p, b in exchange.calls))
        bot.request_stop()
        await task
        self.assertEqual(bot.tripped, "previous breach")
        exchange.close()

    async def test_periodic_risk_check_runs_independently(self):
        exchange, bot, state = fixture()
        with patch.object(app, "RISK_SECONDS", .03), contextlib.redirect_stdout(io.StringIO()):
            task = asyncio.create_task(bot.risk_loop())
            await until(lambda: sum(p == "/v1/perps/balance" for m, p, b in exchange.calls) >= 3)
            bot.request_stop()
            await task
        self.assertFalse(bot.tripped)
        exchange.close()


class WebSocketTests(unittest.IsolatedAsyncioTestCase):
    async def test_api_key_login_waits_for_all_subscriptions(self):
        exchange = FakeExchange()
        channels = {"topOfBooksPerps", "markPricesPerps", "ordersPerps",
                    "fillsPerps", "positionsPerps"}

        class Socket:
            sent = []
            messages = [
                {"type": "loggedIn"},
                *[{"type": "subscribed", "channel": c} for c in sorted(channels)],
                {"type": "update", "channel": "fillsPerps", "data": []},
            ]

            async def send(self, message):
                self.sent.append(json.loads(message))

            async def recv(self):
                return json.dumps(self.messages.pop(0))

        class Connection:
            async def __aenter__(self):
                return socket

            async def __aexit__(self, *args):
                pass

        socket = Socket()
        with patch("websockets.asyncio.client.connect", return_value=Connection()):
            stream = exchange.stream(["TEST-USD.P"])
            self.assertEqual((await anext(stream))["type"], "ready")
            self.assertEqual((await anext(stream))["channel"], "fillsPerps")
            await stream.aclose()
        login = socket.sent[0]["args"]
        self.assertEqual(login["sign"], hmac.new(
            b"test_secret", (login["time"] + "ondo_perps_ws_login").encode(),
            hashlib.sha256,
        ).hexdigest())
        self.assertEqual({r["channel"] for r in socket.sent[1:]}, channels)
        exchange.close()


class ConfigTests(unittest.TestCase):
    def test_defaults_and_invalid_values(self):
        env = {"symbol1": "CRCL", "symbol2": "SNDK", "symbol3": "SOXL",
               "orderSize": "1000", "myLimit": "5000"}
        with patch.dict(os.environ, env, clear=True), patch.object(app, "load_dotenv"):
            config = app.Config.from_env()
            self.assertEqual((config.verse, config.mmr, config.max_leverage),
                             (D("2"), D(".2"), D("5")))
            for name in ("verse", "MMR", "leverage", "orderSize", "myLimit"):
                for value in ("NaN", "Infinity", "0", "-1", "abc"):
                    with self.subTest(name=name, value=value), patch.dict(os.environ, {name: value}):
                        with self.assertRaises(ValueError):
                            app.Config.from_env()


class ReviewRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_static_book_is_verified_without_canceling_unchanged_quotes(self):
        exchange, bot, state = fixture()
        await bot.place_quote(state, "buy")
        await bot.place_quote(state, "sell")
        state.book_at = time.monotonic() - 4
        worker = asyncio.create_task(bot.run_market(state))
        account = asyncio.create_task(bot.account_loop())
        try:
            await until(lambda: any(p == "/v1/perps/depth" for m, p, b in exchange.calls))
            await asyncio.sleep(.02)
            self.assertFalse(any(m == "DELETE" for m, p, b in exchange.calls))
            self.assertEqual(len(state.main), 2)
        finally:
            bot.request_stop()
            await asyncio.wait_for(asyncio.gather(worker, account), 4)
            exchange.close()

    async def test_account_snapshot_avoids_individual_order_reads(self):
        exchange, bot, state = fixture()
        await bot.place_quote(state, "buy")
        await bot.place_quote(state, "sell")
        exchange.calls.clear()
        await bot.reconcile()
        self.assertEqual(sum(p == "/v1/perps/positions" for m, p, b in exchange.calls), 1)
        self.assertEqual(sum(p == "/v1/perps/orders" for m, p, b in exchange.calls), 1)
        self.assertFalse(any(p.startswith("/v1/perps/orders/") for m, p, b in exchange.calls))
        exchange.close()

    async def test_cancel_failure_on_buy_still_cancels_sell(self):
        exchange, bot, state = fixture()
        await bot.place_quote(state, "buy")
        await bot.place_quote(state, "sell")
        request = exchange._request

        def fail_buy(method, path, **kwargs):
            if method == "DELETE" and path.endswith("/1"):
                raise adapter.OndoAPIError("cancel temporarily unavailable")
            return request(method, path, **kwargs)

        with patch.object(exchange, "_request", side_effect=fail_buy):
            await bot.cancel_main(state)
        self.assertIn("buy", state.main)
        self.assertNotIn("sell", state.main)
        self.assertEqual(exchange.orders["2"]["status"], "canceled")
        exchange.close()

    async def test_global_leverage_stops_all_three_markets(self):
        clients = {}
        for base in ("AAA", "BBB", "CCC"):
            exchange = FakeExchange()
            market = copy.deepcopy(exchange.market("TEST"))
            market.update(id=base + "-USD.P", symbol=base + "/USDT:USDT", base=base)
            exchange._markets = {market["symbol"]: market}
            exchange._markets_by_id = {market["id"]: market}
            clients[market["symbol"]] = exchange
        risk_client = next(iter(clients.values()))
        config = app.Config(("AAA", "BBB", "CCC"), D("1000"), D("5000"),
                            D("2"), D(".2"), D("5"))
        bot = app.PostOnlyBot(config, risk_client, clients)
        bot.ws_ready = bot.risk_ok = True
        bot.generation = 1
        for state in bot.states.values():
            state.synced_generation = 1
            bot.set_book(state, clients[state.symbol].book)
            state.position_dirty = False
        workers = [asyncio.create_task(bot.run_market(s)) for s in bot.states.values()]
        risk = None
        try:
            await until(lambda: all(len(s.main) == 2 for s in bot.states.values()))
            # balance.leverage 是全账户值，包含配置之外的持仓。
            risk_client.risk["leverage"] = "6"
            with contextlib.redirect_stdout(io.StringIO()):
                risk = asyncio.create_task(bot.risk_loop())
                await until(lambda: bool(bot.tripped) and
                            all(not s.main for s in bot.states.values()))
            self.assertEqual(sum(len(c.posts()) for c in clients.values()), 6)
            self.assertTrue(all(not bot.quote_allowed(s) for s in bot.states.values()))
        finally:
            bot.request_stop()
            await asyncio.wait_for(asyncio.gather(*workers), 4)
            if risk:
                await risk
            for client in clients.values():
                client.close()


class MemoryRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_terminal_orders_and_fill_ids_are_released(self):
        exchange, bot, state = fixture()
        await bot.place_quote(state, "buy")
        fill = exchange.fill("1", "1")
        bot.accept_fills(state, [fill])
        await bot.cancel_main(state)
        await bot.send_hedges(state)
        self.assertFalse(state.tracked)
        self.assertFalse(state.by_id)
        self.assertFalse(state.seen)
        posts = len(exchange.posts())
        bot.accept_fills(state, [fill])
        await bot.send_hedges(state)
        self.assertEqual(len(exchange.posts()), posts)
        exchange.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
