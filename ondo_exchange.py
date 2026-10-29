import asyncio
import hashlib
import hmac
import json
import os
import re
import time
import uuid
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_HALF_UP
from urllib.parse import urlsplit


class OndoFatalError(RuntimeError):
    """订单状态无法安全恢复，需要停止策略。"""


class OndoAPIError(RuntimeError):
    def __init__(self, message, code="", uncertain=False):
        super().__init__(message)
        self.code = code
        self.uncertain = uncertain


class OndoExchange:
    id = "ondo"

    def __init__(
        self, api_key_id, api_secret,
        api_url="https://api.ondoperps.xyz", timeout=10,
    ):
        try:
            import requests
        except ImportError as exc:
            raise RuntimeError(
                "requests is not installed; use requirements-ondo.txt"
            ) from exc

        if not api_key_id or not api_secret:
            raise ValueError("Missing ONDO_API_KEY_ID or ONDO_API_SECRET")
        url = urlsplit(api_url)
        if (
            url.scheme != "https"
            or url.netloc not in (
                "api.ondoperps.xyz", "api.ondoperps-sandbox.xyz"
            )
            or url.path not in ("", "/")
            or url.query or url.fragment
        ):
            raise ValueError("ONDO_API_URL must be an official Ondo API URL")
        if not 0 < timeout < float("inf"):
            raise ValueError("ONDO_TIMEOUT must be positive and finite")

        self.api_key_id = api_key_id
        self._api_secret = api_secret
        self.api_url = api_url.rstrip("/")
        self.timeout = timeout
        self._requests = requests
        self._session = requests.Session()
        self._markets = {}
        self._markets_by_id = {}
        self._closed = False

    @classmethod
    def from_env(cls):
        api_key_id = os.getenv("ONDO_API_KEY_ID", "").strip().strip("'\"")
        api_secret = os.getenv("ONDO_API_SECRET", "").strip().strip("'\"")
        api_url = os.getenv(
            "ONDO_API_URL", "https://api.ondoperps.xyz"
        ).strip().strip("'\"")
        timeout = float(os.getenv("ONDO_TIMEOUT", "10").strip().strip("'\""))
        return cls(api_key_id, api_secret, api_url, timeout)

    def _request(self, method, path, params=None, body=None, private=True):
        if self._closed:
            raise RuntimeError("Ondo client is closed")

        method = method.upper()
        body_text = (
            json.dumps(body, separators=(",", ":")) if body is not None else ""
        )
        request = self._requests.Request(
            method, self.api_url + path, params=params,
            data=body_text.encode("utf-8") if body is not None else None,
            headers={"Content-Type": "application/json"},
        )
        prepared = self._session.prepare_request(request)
        if private:
            timestamp = str(int(time.time() * 1000))
            # 使用最终发送的 URI 和正文，避免编码或空格导致签名不一致。
            message = timestamp + method.upper() + prepared.path_url + body_text
            signature = hmac.new(
                self._api_secret.encode(), message.encode(), hashlib.sha256
            ).hexdigest()
            prepared.headers.update({
                "ONDO-KEY-ID": self.api_key_id,
                "ONDO-TIMESTAMP": timestamp,
                "ONDO-SIGN": signature,
            })

        settings = self._session.merge_environment_settings(
            prepared.url, {}, None, None, None
        )
        try:
            response = self._session.send(
                prepared, timeout=self.timeout, allow_redirects=False, **settings
            )
        except self._requests.RequestException as exc:
            raise OndoAPIError(
                f"Ondo {method} request failed: {type(exc).__name__}",
                uncertain=method in {"POST", "DELETE"},
            ) from None

        with response:
            status = response.status_code
            try:
                payload = response.json()
            except ValueError:
                raise OndoAPIError(
                    f"Ondo returned invalid JSON (HTTP {status})",
                    uncertain=method in {"POST", "DELETE"},
                ) from None
            if (
                not 200 <= status < 300
                or not isinstance(payload, dict)
                or payload.get("success") is not True
            ):
                code, message = "", "invalid response"
                if isinstance(payload, dict):
                    code = payload.get("error_code", "")
                    message = payload.get("error", "request failed")
                detail = f"Ondo HTTP {status}: {code} {message}"
                for secret in (self.api_key_id, self._api_secret):
                    detail = detail.replace(secret, "<redacted>")
                raise OndoAPIError(
                    detail, code=code,
                    uncertain=method in {"POST", "DELETE"} and (
                        status >= 500 or 300 <= status < 400
                        or not isinstance(payload, dict)
                        or payload.get("success") is not False
                    ),
                )
            return payload

    def load_markets(self):
        response = self._request("GET", "/v1/markets")
        markets = {}
        for detail in response["result"]["perps"]["tradingPairs"]:
            market_id = detail["market"]
            if (
                not market_id.endswith(".P") or detail.get("disabled")
                or detail.get("tradingState", "TRADING") != "TRADING"
            ):
                continue
            amount_step = Decimal(detail["baseIncrement"])
            price_step = Decimal(detail["quoteIncrement"])
            if any(not step.is_finite() or step <= 0
                   for step in (amount_step, price_step)):
                raise ValueError(f"Invalid Ondo increments for {market_id}")
            pair = detail.get("pair") or {}
            base = (pair.get("base") or market_id.rsplit("-", 1)[0]).upper()
            # USDT 格式仅作配置别名，请求仍使用原生市场 ID。
            symbol = f"{base}/USDT:USDT"
            if symbol in markets:
                raise ValueError(f"Ambiguous Ondo symbol {symbol!r}")
            markets[symbol] = {
                "id": market_id,
                "symbol": symbol,
                "base": base,
                "quote": pair.get("quote") or "USD",
                "swap": True,
                "active": True,
                "contractSize": 1,
                "precision": {
                    "amount": str(amount_step), "price": str(price_step),
                },
                "limits": {
                    "amount": {"min": float(amount_step), "max": None},
                    "cost": {"min": None, "max": None},
                },
                "info": detail,
            }
        if not markets:
            raise RuntimeError("Ondo returned no active perpetual markets")
        self._markets = markets
        self._markets_by_id = {m["id"].upper(): m for m in markets.values()}
        return markets

    def market(self, symbol):
        if not self._markets:
            self.load_markets()
        symbol = symbol.strip().upper()
        market = self._markets.get(symbol) or self._markets_by_id.get(symbol)
        if market is None:
            matches = [m for m in self._markets.values() if m["base"] == symbol]
            if len(matches) == 1:
                market = matches[0]
        if market is None:
            raise ValueError(
                f"Ondo perpetual market {symbol!r} was not found; "
                "use a symbol such as BTC/USDT:USDT (also accepts BTC-USD.P)"
            )
        return market

    def amount_to_precision(self, symbol, amount):
        step = Decimal(self.market(symbol)["precision"]["amount"])
        value = Decimal(str(amount))
        if not value.is_finite() or value < 0:
            raise ValueError("Ondo amount must be non-negative and finite")
        value = (value / step).to_integral_value(rounding=ROUND_DOWN) * step
        return format(value.quantize(step), "f")

    def price_to_precision(self, symbol, price, rounding=ROUND_HALF_UP):
        step = Decimal(self.market(symbol)["precision"]["price"])
        value = Decimal(str(price))
        if not value.is_finite() or value < 0:
            raise ValueError("Ondo price must be non-negative and finite")
        value = (value / step).to_integral_value(rounding=rounding) * step
        return format(value.quantize(step), "f")

    def fetch_order_book(self, symbol, limit=5):
        market = self.market(symbol)
        response = self._request(
            "GET", "/v1/perps/depth",
            params={"market": market["id"], "depth": max(1, min(int(limit), 100))},
            private=False,
        )
        book = response["result"]
        bids = [[float(p), float(q)] for p, q in book["bids"] if Decimal(q) > 0]
        asks = [[float(p), float(q)] for p, q in book["asks"] if Decimal(q) > 0]
        bids.sort(key=lambda level: level[0], reverse=True)
        asks.sort(key=lambda level: level[0])
        return {"bids": bids, "asks": asks, "info": book}

    def fetch_ticker(self, symbol):
        market = self.market(symbol)
        response = self._request("GET", "/v1/perps/mark_prices", private=False)
        detail = response["result"][market["id"]]
        mark = float(detail.get("markPrice") or detail.get("price") or 0)
        return {"mark": mark, "last": None, "close": mark}

    def fetch_positions(self, symbols=None):
        if not self._markets:
            self.load_markets()
        response = self._request("GET", "/v1/perps/positions")
        requested = symbols if symbols is not None else list(self._markets)
        symbols_by_market = {}
        for symbol in requested:
            market = self.market(symbol)
            symbols_by_market[market["id"]] = market["symbol"]
        positions = []
        for position in response["result"]:
            symbol = symbols_by_market.get(position["market"])
            if symbol is None:
                continue
            contracts = abs(Decimal(position["netQuantity"]))
            side = position["direction"]
            if side not in {"long", "short", "neutral"}:
                raise ValueError("Unknown Ondo position direction")
            size = -contracts if side == "short" else contracts
            positions.append({
                "symbol": symbol,
                "contracts": float(contracts),
                "side": side,
                "unrealizedPnl": float(position["unrealizedPnl"]),
                "liquidationPrice": float(position.get("liquidationPrice") or 0),
                "info": {**position, "positionAmt": str(size)},
            })
        return positions

    def _parse_order(self, order):
        order_id = str(order["orderId"] or "")
        amount = Decimal(order["size"])
        filled = Decimal(order["filledSize"])
        if (
            not order_id or not amount.is_finite() or not filled.is_finite()
            or amount <= 0 or not 0 <= filled <= amount
        ):
            raise ValueError("Invalid Ondo order ID or quantity")
        status = order["status"]
        if status not in {"open", "pending", "untriggered", "canceled", "fullyfilled"}:
            raise ValueError(f"Unknown Ondo order status: {status!r}")
        if status == "fullyfilled":
            if filled != amount:
                raise ValueError("Inconsistent Ondo fullyfilled quantity")
            status = "filled"
        return {
            "id": order_id,
            "clientOrderId": order.get("clientOrderId"),
            "symbol": self.market(order["market"])["symbol"],
            "price": float(order["price"] or 0),
            "amount": float(amount),
            "filled": float(filled),
            "remaining": float(amount - filled),
            "side": order["side"],
            "status": status,
            "info": order,
        }

    def fetch_open_orders(self, symbol):
        market = self.market(symbol)
        params = {"market": market["id"], "status": "open", "limit": 1000}
        orders, cursors = {}, set()
        while True:
            page = self._request("GET", "/v1/perps/orders", params=params)
            if not isinstance(page["result"], list):
                raise ValueError("Invalid Ondo open orders response")
            for raw in page["result"]:
                if raw["market"] != market["id"]:
                    raise ValueError("Ondo returned orders for another market")
                if raw["status"] in {"open", "pending", "untriggered"}:
                    order = self._parse_order(raw)
                    orders[order["id"]] = order
            cursor = (page.get("pageInfo") or {}).get("nextCursor")
            if not cursor:
                return list(orders.values())
            if cursor in cursors:
                raise RuntimeError("Ondo returned a repeated pagination cursor")
            cursors.add(cursor)
            params["cursor"] = cursor

    def fetch_order_statuses(self, symbol, order_ids):
        market = self.market(symbol)
        statuses = {}
        for order_id in sorted(str(value) for value in order_ids):
            try:
                response = self._request("GET", f"/v1/perps/orders/{order_id}")
            except OndoAPIError as exc:
                if exc.code == "order_not_found":
                    continue
                raise
            order = self._parse_order(response["result"])
            if order["symbol"] != market["symbol"] or order["id"] != order_id:
                raise ValueError("Ondo returned a different order")
            statuses[order_id] = order["status"]
        return statuses

    def create_order(self, symbol, order_type, side, amount, price, params=None):
        if order_type != "limit" or side not in {"buy", "sell"}:
            raise ValueError("Ondo adapter requires a buy or sell limit order")
        params = params or {}
        if str(params.get("timeInForce", "GTC")).upper() != "GTC":
            raise ValueError("Ondo adapter requires GTC orders")
        for flag in ("postOnly", "reduceOnly"):
            if not isinstance(params.get(flag, False), bool):
                raise ValueError(f"{flag} must be a boolean")
        market = self.market(symbol)
        amount = self.amount_to_precision(symbol, amount)
        price = self.price_to_precision(symbol, price)
        if Decimal(amount) <= 0 or Decimal(price) <= 0:
            raise ValueError("Ondo order amount and price must be positive")
        client_id = params.get("clientOrderId") or "grid_" + uuid.uuid4().hex
        if not isinstance(client_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", client_id):
            raise ValueError("Invalid clientOrderId")
        body = {
            "market": market["id"], "type": "limit", "side": side,
            "size": amount, "price": price, "clientOrderId": client_id,
            "timeInForce": "GTC",
            "postOnly": params.get("postOnly", False),
            "reduceOnly": params.get("reduceOnly", False),
        }
        # POST 只发一次；结果不确定时按客户端 ID 确认，禁止盲目重发。
        for attempt in range(4):
            if attempt > 1:
                time.sleep(0.5)
            try:
                if attempt == 0:
                    response = self._request("POST", "/v1/perps/orders", body=body)
                else:
                    response = self._request(
                        "GET", f"/v1/perps/orders/client:{client_id}"
                    )
            except OndoAPIError as exc:
                if attempt == 0 and not exc.uncertain and exc.code != "clientOrderID_collision":
                    raise
                continue
            try:
                order = self._parse_order(response["result"])
            except (KeyError, TypeError, ValueError, InvalidOperation):
                continue
            if (
                order["symbol"] == market["symbol"]
                and order["clientOrderId"] == client_id
                and order["side"] == side
                and Decimal(order["info"]["size"]) == Decimal(amount)
                and Decimal(order["info"]["price"]) == Decimal(price)
            ):
                return order
        raise OndoFatalError(
            f"Cannot confirm Ondo order client_id={client_id}; stopping to avoid duplicates"
        )

    def fetch_order(self, order_id, symbol):
        order = self._parse_order(
            self._request("GET", f"/v1/perps/orders/{order_id}")["result"]
        )
        if order["symbol"] != self.market(symbol)["symbol"]:
            raise ValueError("Ondo returned an order for another market")
        if not str(order_id).startswith("client:") and order["id"] != str(order_id):
            raise ValueError("Ondo returned a different order")
        if str(order_id).startswith("client:") and order["clientOrderId"] != str(order_id)[7:]:
            raise ValueError("Ondo returned a different client order")
        return order

    def cancel_order(self, order_id, symbol):
        try:
            result = self._request("DELETE", f"/v1/perps/orders/{order_id}")
        except OndoAPIError as exc:
            if not exc.uncertain and exc.code not in {
                "order_already_cancelled", "order_not_in_cancellable_state",
                "order_already_filled",
            }:
                raise
        else:
            raw = result.get("result")
            if isinstance(raw, dict) and raw.get("orderId"):
                order = self._parse_order(raw)
                if order["symbol"] != self.market(symbol)["symbol"]:
                    raise ValueError("Ondo canceled an order for another market")
                if not str(order_id).startswith("client:") and order["id"] != str(order_id):
                    raise ValueError("Ondo canceled a different order")
                if str(order_id).startswith("client:") and order["clientOrderId"] != str(order_id)[7:]:
                    raise ValueError("Ondo canceled a different client order")
                return order
        return self.fetch_order(order_id, symbol)

    def fetch_order_fills(self, order_id, symbol):
        market_id = self.market(symbol)["id"]
        fills = self._request("GET", f"/v1/perps/orders/{order_id}/fills")["result"]
        # 空成交可能序列化为 null；累计成交校验仍由主程序负责。
        if fills is None:
            return []
        if not isinstance(fills, list):
            detail = type(fills).__name__
            if isinstance(fills, dict):
                detail += f" keys={list(fills)[:8]}"
            raise ValueError(f"Invalid Ondo fills response: expected list or null, got {detail}")
        for fill in fills:
            if (
                fill["market"] != market_id
                or (not str(order_id).startswith("client:") and fill["orderId"] != str(order_id))
            ):
                raise ValueError("Ondo returned fills for another order")
            for name in ("price", "size"):
                value = Decimal(fill[name])
                if not value.is_finite() or value <= 0:
                    raise ValueError(f"Invalid fill {name}")
        return fills

    def fetch_account_risk(self):
        raw = self._request("GET", "/v1/perps/balance")["result"]
        result = {"info": raw}
        for name in ("marginRatio", "leverage", "marginBalance"):
            value = Decimal(raw[name])
            if not value.is_finite() or (name != "marginBalance" and value < 0):
                raise ValueError(f"Invalid Ondo risk metric: {name}")
            result[name] = value
        return result

    async def stream(self, markets):
        """同一连接订阅行情和私有成交；重连由主程序负责。"""
        from websockets.asyncio.client import connect

        channels = {
            "topOfBooksPerps", "markPricesPerps",
            "ordersPerps", "fillsPerps", "positionsPerps",
        }
        async with connect(
            self.api_url.replace("https://", "wss://", 1) + "/ws",
            open_timeout=self.timeout, close_timeout=3,
            ping_interval=10, ping_timeout=5, max_size=4 * 1024 * 1024,
        ) as ws:
            timestamp = str(int(time.time() * 1000))
            signature = hmac.new(
                self._api_secret.encode(),
                (timestamp + "ondo_perps_ws_login").encode(),
                hashlib.sha256,
            ).hexdigest()
            await ws.send(json.dumps({
                "op": "login",
                "args": {"key": self.api_key_id, "time": timestamp, "sign": signature},
            }))
            started = last_ping = time.monotonic()
            waiting_pong = None
            subscribed = set()
            logged_in = False
            while True:
                now = time.monotonic()
                if len(subscribed) != len(channels) and now - started > self.timeout:
                    raise OndoAPIError("WebSocket login/subscription timed out")
                if waiting_pong is not None and now - waiting_pong > 5:
                    raise OndoAPIError("WebSocket heartbeat timed out")
                if now - last_ping >= 10 and waiting_pong is None:
                    await ws.send('{"op":"ping"}')
                    last_ping = waiting_pong = now
                try:
                    message = json.loads(await asyncio.wait_for(ws.recv(), timeout=1))
                except asyncio.TimeoutError:
                    continue
                if message.get("type") == "error":
                    # 不记录原始登录响应，避免服务器回显密钥。
                    raise OndoAPIError("WebSocket rejected login or subscription")
                if message.get("type") == "pong":
                    waiting_pong = None
                elif message.get("type") == "loggedIn" and not logged_in:
                    logged_in = True
                    for channel in sorted(channels):
                        request = {"op": "subscribe", "channel": channel}
                        if channel != "positionsPerps":
                            request["markets"] = list(markets)
                        await ws.send(json.dumps(request))
                elif message.get("type") == "subscribed":
                    channel = message.get("channel")
                    if channel in channels and channel not in subscribed:
                        subscribed.add(channel)
                        if subscribed == channels:
                            yield {"type": "ready"}
                elif message.get("type") == "update":
                    yield message

    def cancel_all_orders(self, symbol):
        market = self.market(symbol)
        return self._request(
            "DELETE", "/v1/perps/orders", params={"market": market["id"]}
        )

    def close(self):
        if not self._closed:
            self._session.close()
            self._closed = True
