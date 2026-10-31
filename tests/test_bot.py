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
        self.book = {'bids': [['99.99', '1'], ['99.98', '20'], ['99.96', '5']],
                     'asks': [['100.01', '1'], ['100.02', '20'], ['100.04', '5']]}
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
        if path == '/v1/perps/orders/batch':
            self.calls.append((method, path, copy.deepcopy(body)))
            if method == 'POST':
                added, failed = [], []
                for item in body['orders']:
                    try:
                        added.append(self._request('POST', '/v1/perps/orders', body=item)['result'])
                    except adapter.OndoAPIError as exc:
                        if exc.uncertain:
                            raise
                        failed.append({'order': item, 'errorCode': exc.code})
                return {'result': {'addedOrders': added, 'failedOrders': failed}}
            canceled = []
            for oid in params['orderIDs'].split(','):
                try:
                    canceled.append(self._request('DELETE', '/v1/perps/orders/' + oid)['result'])
                except adapter.OndoAPIError:
                    pass
            return {'result': {'successfulCancels': canceled}}
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


def push_book(exchange, bot, state, book=None, own=True):
    from datetime import datetime, timezone
    raw = copy.deepcopy(book or exchange.book)
    stamp = max(time.time(), state.depth_time + .001, state.book_time + .001)
    raw['time'] = datetime.fromtimestamp(stamp, timezone.utc).isoformat()
    raw['market'] = state.market['id']
    if own:
        for order in exchange.orders.values():
            if order['status'] in {'open', 'pending'}:
                side = 'bids' if order['side'] == 'buy' else 'asks'
                qty = D(order['size']) - D(order['filledSize'])
                raw[side].append([order['price'], str(qty)])
    bot.set_book(state, raw)
    state.wake.set()
    return raw


def fixture():
    exchange = FakeExchange()
    config = app.Config((app.MarketConfig('TEST', D('1000'), D('5000')),))
    bot = app.PostOnlyBot(config, exchange, {"TEST/USDT:USDT": exchange})
    state = next(iter(bot.states.values()))
    bot.risk_ok = bot.ws_ready = True
    bot.generation = state.synced_generation = 1
    push_book(exchange, bot, state)
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
        channels = {"topOfBooksPerps", "depthBooksPerps", "markPricesPerps", "ordersPerps",
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
        depth = next(r for r in socket.sent if r.get('channel') == 'depthBooksPerps')
        self.assertEqual(depth['limit'], 0)
        self.assertEqual(depth['depthLevels'], '0.01')
        self.assertEqual(depth['markets'], ['TEST-USD.P'])
        exchange.close()


