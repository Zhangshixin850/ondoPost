import unittest
from decimal import Decimal
from unittest.mock import Mock

from ondo_exchange import OndoAPIError, OndoExchange
from postOnly import Config, PostOnlyBot


class OrderFillsTests(unittest.TestCase):
    def setUp(self):
        self.exchange = OndoExchange.__new__(OndoExchange)
        self.exchange.market = Mock(return_value={"id": "CRCL-USD.P"})
        self.exchange._request = Mock()

    def test_null_means_no_fills(self):
        self.exchange._request.return_value = {"result": None}
        self.assertEqual(self.exchange.fetch_order_fills("order1", "CRCL"), [])

    def test_empty_array_means_no_fills(self):
        self.exchange._request.return_value = {"result": []}
        self.assertEqual(self.exchange.fetch_order_fills("order1", "CRCL"), [])

    def test_nonempty_fills_are_preserved(self):
        fills = [{"id": "fill1", "orderId": "order1", "market": "CRCL-USD.P",
                  "price": "80.61", "size": "0.1"}]
        self.exchange._request.return_value = {"result": fills}
        self.assertEqual(self.exchange.fetch_order_fills("order1", "CRCL"), fills)

    def test_unexpected_shape_is_not_treated_as_empty(self):
        for value in ({"unexpected": []}, "invalid", 0):
            with self.subTest(value=value):
                self.exchange._request.return_value = {"result": value}
                with self.assertRaisesRegex(ValueError, "Invalid Ondo fills response"):
                    self.exchange.fetch_order_fills("order1", "CRCL")

    def test_unexpected_shape_reports_keys_without_values(self):
        self.exchange._request.return_value = {"result": {"unexpected": "private-value"}}
        with self.assertRaisesRegex(ValueError, "dict keys=.*unexpected") as caught:
            self.exchange.fetch_order_fills("order1", "CRCL")
        self.assertNotIn("private-value", str(caught.exception))

    def test_other_order_is_rejected(self):
        self.exchange._request.return_value = {"result": [
            {"orderId": "another_order", "market": "CRCL-USD.P",
             "price": "80.61", "size": "0.1"}]}
        with self.assertRaisesRegex(ValueError, "another order"):
            self.exchange.fetch_order_fills("order1", "CRCL")

    def test_other_market_is_rejected(self):
        self.exchange._request.return_value = {"result": [
            {"orderId": "order1", "market": "SOXL-USD.P",
             "price": "101.69", "size": "0.1"}]}
        with self.assertRaisesRegex(ValueError, "another order"):
            self.exchange.fetch_order_fills("order1", "CRCL")


class MainOrderSyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.market = {
            "id": "CRCL-USD.P", "symbol": "CRCL/USDT:USDT",
            "precision": {"amount": "0.1", "price": "0.01"},
        }
        self.exchange = OndoExchange.__new__(OndoExchange)
        self.exchange.market = Mock(return_value=self.market)
        self.exchange._request = Mock(side_effect=self.request)
        self.raw = None
        self.fills = None
        config = Config(("CRCL", "SNDK", "SOXL"), Decimal("1000"), Decimal("5000"),
                        Decimal("2"), Decimal("0.2"), Decimal("5"))
        self.bot = PostOnlyBot(config, self.exchange, {self.market["symbol"]: self.exchange})
        self.state = self.bot.states[self.market["symbol"]]
        self.bot.risk_ok = self.bot.ws_ready = True
        self.bot.generation = self.state.synced_generation = 1
        self.state.position_dirty = False
        self.bot.set_book(self.state, {"bids": [["80.64", "100"]],
                                       "asks": [["80.66", "100"]]})
        await self.bot.place_quote(self.state, "buy")
        self.entry = self.state.tracked[self.state.main["buy"]]
        self.exchange._request.reset_mock()

    def request(self, method, path, params=None, body=None, private=True):
        if method == "POST":
            self.raw = {"orderId": "order1", "clientOrderId": body["clientOrderId"],
                        "market": body["market"], "side": body["side"],
                        "price": body["price"], "size": body["size"],
                        "filledSize": "0", "status": "open"}
        if path.endswith("/fills"):
            return {"result": self.fills}
        if method == "DELETE":
            self.raw["status"] = "canceled"
        return {"result": self.raw.copy()}

    def assert_no_fill_query(self):
        self.assertFalse(any(call.args[1].endswith("/fills")
                             for call in self.exchange._request.call_args_list))

    async def test_unfilled_open_order_does_not_query_fills(self):
        await self.bot.sync_main(self.state, self.entry)
        self.assert_no_fill_query()
        self.assertIn("buy", self.state.main)

    async def test_unfilled_cancel_succeeds_without_fill_query(self):
        await self.bot.cancel_main(self.state)
        self.assert_no_fill_query()
        self.assertFalse(self.state.main)
        self.assertFalse(self.state.problem)

    async def test_null_does_not_hide_unaccounted_partial_fill(self):
        self.raw["filledSize"] = "1"
        with self.assertRaisesRegex(OndoAPIError, "成交明细尚未齐全"):
            await self.bot.sync_main(self.state, self.entry)
        self.assertIn("buy", self.state.main)
        self.assertFalse(self.state.hedges)

    async def test_partial_fill_is_queued_for_hedging_before_releasing_slot(self):
        self.raw.update(filledSize="1", status="canceled")
        self.fills = [{"id": "fill1", "orderId": "order1",
                       "clientOrderId": self.raw["clientOrderId"],
                       "market": "CRCL-USD.P", "side": "buy",
                       "price": self.raw["price"], "size": "1"}]
        await self.bot.sync_main(self.state, self.entry)
        self.assertFalse(self.state.main)
        self.assertEqual(self.state.hedges["fill1"]["size"], Decimal("1"))
        self.assertEqual(self.state.hedges["fill1"]["side"], "sell")

    async def test_already_received_fill_does_not_need_rest_history(self):
        self.raw.update(filledSize="1", status="canceled")
        self.bot.accept_fills(self.state, [{"id": "fill1", "orderId": "order1",
            "clientOrderId": self.raw["clientOrderId"], "market": "CRCL-USD.P",
            "side": "buy", "price": self.raw["price"], "size": "1"}])
        await self.bot.sync_main(self.state, self.entry)
        self.assert_no_fill_query()
        self.assertFalse(self.state.main)
        self.assertEqual(len(self.state.hedges), 1)


if __name__ == "__main__":
    unittest.main()
