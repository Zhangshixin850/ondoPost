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


