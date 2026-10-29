"""Ondo 奖励挂单：python postOnly.py；仅检查配置：python postOnly.py --check。"""

import argparse
import asyncio
import hashlib
import logging
import os
import signal
import sys
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path

from dotenv import load_dotenv

from ondo_exchange import (
    OndoAPIError, OndoExchange, OndoFatalError, OndoRequestSkipped, RequestLimiter,
)


BPS = Decimal("10000")
TARGET = Decimal("4")
RISK_SECONDS = 10
RECONCILE_SECONDS = 30
BOOK_SECONDS = 3
MAIN_PREFIX = "po_q_"
HEDGE_PREFIX = "po_h_"
TERMINAL = {"filled", "canceled"}
log = logging.getLogger("postOnly")


@dataclass
class Config:
    symbols: tuple
    order_size: Decimal
    position_limit: Decimal
    verse: Decimal
    mmr: Decimal
    max_leverage: Decimal
    near_limit: Decimal = Decimal("2")
    far_limit: Decimal = Decimal("4.9")
    rps: int = 5

    @classmethod
    def from_env(cls):
        load_dotenv(Path(__file__).with_name(".env"), encoding="utf-8-sig")
        symbols = tuple(value for i in (1, 2, 3)
                        if (value := os.getenv(f"symbol{i}", "").strip().upper()))
        if not symbols or len(set(symbols)) != len(symbols):
            raise ValueError("至少填写一个 symbol，非空市场不能重复")
        numbers = []
        for name, default in (
            ("orderSize", ""), ("myLimit", ""),
            ("verse", "2"), ("MMR", "0.2"), ("leverage", "5"),
            ("nearLimit", "2"), ("farLimit", "4.9"),
        ):
            try:
                value = Decimal(os.getenv(name, default).strip())
            except InvalidOperation:
                raise ValueError(f"{name} 必须是有效数字") from None
            if not value.is_finite() or value <= 0:
                raise ValueError(f"{name} 必须是有限正数")
            numbers.append(value)
        if numbers[2] >= BPS:
            raise ValueError("verse 必须小于 10000bps")
        if not numbers[5] <= TARGET <= numbers[6] or numbers[5] >= numbers[6]:
            raise ValueError("挂单区间必须满足 0 < nearLimit <= 4 <= farLimit，且 nearLimit < farLimit")
        try:
            rps = int(os.getenv("rps", "5").strip())
        except ValueError:
            raise ValueError("rps 必须是正整数") from None
        if rps < 1:
            raise ValueError("rps 必须是正整数")
        return cls(symbols, *numbers, rps)


@dataclass
class MarketState:
    market: dict
    main: dict = field(default_factory=dict)
    tracked: dict = field(default_factory=dict)
    by_id: dict = field(default_factory=dict)
    fills: deque = field(default_factory=deque)
    updates: deque = field(default_factory=deque)
    seen: set = field(default_factory=set)
    hedges: dict = field(default_factory=dict)
    partial: set = field(default_factory=set)
    retry_at: dict = field(default_factory=dict)
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    book: tuple = ()
    book_at: float = 0
    position: Decimal = Decimal("0")
    mark: Decimal = Decimal("0")
    position_dirty: bool = True
    position_seq: int = 0
    position_refresh_at: float = 0
    book_retry_at: float = 0
    book_refreshing: bool = False
    synced_generation: int = -1
    stopped: str = ""
    fatal: str = ""
    problem: str = ""

    @property
    def symbol(self):
        return self.market["symbol"]


class PostOnlyBot:
    def __init__(self, config, risk_client, clients, account_client=None):
        self.config = config
        self.risk_client = risk_client
        self.clients = clients
        self.account_client = account_client or risk_client
        limiter = getattr(risk_client, "limiter", None)
        self.limiter = limiter if limiter and limiter.rps == config.rps else RequestLimiter(config.rps)
        for client in {risk_client, self.account_client, *clients.values()}:
            client.limiter = self.limiter
        self.states = {
            symbol: MarketState(client.market(symbol))
            for symbol, client in clients.items()
        }
        self.by_market = {s.market["id"]: s for s in self.states.values()}
        self.risk_ok = False
        self.tripped = ""
        self.ws_ready = False
        self.generation = 0
        self.closing = False
        self.shutdown_deadline = 0
        self.stop_event = asyncio.Event()
        self.exit_code = 0
        self.account_wake = asyncio.Event()
        self.need_reconcile = True
        self.recovering = False
        self.last_cooldown = -1
        self.reconcile_lock = asyncio.Lock()
        self.last_risk = None
        self.risk_at = 0
        self.risk_refresh = True

    def primary_paused(self, state):
        return (
            self.closing or bool(self.tripped) or not self.risk_ok
            or self.recovering or self.limiter.cooling
            or not self.ws_ready or state.synced_generation != self.generation
            or bool(state.stopped or state.fatal or state.problem)
            or any(h["attempts"] or h["unknown"] for h in state.hedges.values())
        )

    def quote_allowed(self, state):
        return (
            not self.primary_paused(state) and not state.hedges
            and not state.position_dirty
            and not state.partial and not state.fills and not state.updates
        )

    async def rest(self, client, method, *args, priority=2, valid=None, post_valid=None):
        def allowed():
            return ((not self.closing or time.monotonic() < self.shutdown_deadline)
                    and (valid is None or valid()))
        def execute():
            with client.request_context(priority, allowed, post_valid):
                return getattr(client, method)(*args)
        try:
            return await asyncio.to_thread(execute)
        except OndoAPIError as exc:
            if exc.status == 429 or exc.code == "too_many_requests":
                if not self.limiter.cooling:
                    self.limiter.defer(exc.retry_after)
                self.recovering = self.need_reconcile = self.risk_refresh = True
                self.risk_ok = False
                if self.last_cooldown != self.limiter.cooldown_generation:
                    self.last_cooldown = self.limiter.cooldown_generation
                    log.warning("账户限流，所有 REST 冷却 %.1f 秒",
                                max(0, self.limiter.cooldown_until - time.monotonic()))
                self.account_wake.set()
                for state in self.states.values():
                    state.wake.set()
            raise

    def request_stop(self):
        if not self.closing:
            self.closing = True
            self.shutdown_deadline = time.monotonic() + 60
            self.stop_event.set()
            self.account_wake.set()
            with self.limiter.condition:
                self.limiter.condition.notify_all()
            for state in self.states.values():
                state.wake.set()

    def set_book(self, state, raw):
        bids = [Decimal(str(p)) for p, q in raw.get("bids", []) if Decimal(str(q)) > 0]
        asks = [Decimal(str(p)) for p, q in raw.get("asks", []) if Decimal(str(q)) > 0]
        if not bids or not asks:
            raise ValueError("盘口缺少买价或卖价")
        bid, ask = max(bids), min(asks)
        if not bid.is_finite() or not ask.is_finite() or not 0 < bid < ask:
            raise ValueError("盘口价格无效")
        state.book = (bid, ask)
        state.book_at = time.monotonic()

    def observe_order(self, state, raw):
        cid = raw.get("clientOrderId") or state.by_id.get(str(raw["orderId"]))
        entry = state.tracked.get(cid)
        if entry is None:
            return
        if raw["market"] != state.market["id"] or raw["side"] != entry["side"]:
            raise OndoFatalError("主单返回的市场或方向不一致")
        order_id = str(raw["orderId"])
        if entry["id"] and entry["id"] != order_id:
            raise OndoFatalError(f"主单 ID 改变: {cid}")
        filled = Decimal(raw["filledSize"])
        size = Decimal(raw["size"])
        if (
            not filled.is_finite() or not size.is_finite()
            or size != entry["size"] or not 0 <= filled <= size
        ):
            raise OndoFatalError(f"主单数量异常: {cid}")
        entry["id"] = order_id
        entry.setdefault("accepted_at", time.monotonic())
        entry["uncertain"] = False
        state.by_id[order_id] = cid
        status = "filled" if raw["status"] == "fullyfilled" else raw["status"]
        if status not in {"open", "pending", "untriggered", *TERMINAL}:
            raise OndoFatalError(f"未知订单状态: {status}")
        if filled >= entry["filled"]:
            entry["filled"] = filled
            if entry["status"] not in TERMINAL or status in TERMINAL:
                entry["status"] = status
            entry["raw"] = raw
        entry["needs_lookup"] = False
        if filled > 0 and not entry["settled"]:
            state.partial.add(entry["side"])

    def accept_fills(self, state, fills):
        """按逐笔成交去重；先保留待提交记录，失败后也不会漏掉这笔对冲。"""
        for fill in fills:
            cid = fill.get("clientOrderId") or state.by_id.get(str(fill["orderId"]))
            entry = state.tracked.get(cid)
            if entry is None or fill["id"] in state.seen:
                continue
            qty, price = Decimal(str(fill["size"])), Decimal(str(fill["price"]))
            if (
                fill["market"] != state.market["id"] or fill["side"] != entry["side"]
                or not qty.is_finite() or not price.is_finite() or min(qty, price) <= 0
                or entry["seen_size"] + qty > entry["size"]
            ):
                raise OndoFatalError(f"成交数据异常: {fill['id']}")
            state.seen.add(fill["id"])
            entry["fill_ids"].add(fill["id"])
            entry["seen_size"] += qty
            state.partial.add(entry["side"])
            # 仓位只采用交易所快照，不能在已包含本笔成交的仓位上再次累加。
            if fill.get("_position_seq", -1) >= state.position_seq or "_position_seq" not in fill:
                state.position_seq += 1
                state.position_dirty = True
                state.position_refresh_at = time.monotonic() + 1
                self.account_wake.set()
            hedge_side = "sell" if fill["side"] == "buy" else "buy"
            offset = self.config.verse / BPS
            target = price * (1 + offset if hedge_side == "sell" else 1 - offset)
            client = self.clients[state.symbol]
            target = Decimal(client.price_to_precision(
                state.symbol, target,
                rounding=ROUND_CEILING if hedge_side == "sell" else ROUND_FLOOR,
            ))
            if target <= 0:
                raise OndoFatalError("反向挂单取整后的价格必须为正")
            state.hedges[fill["id"]] = {
                "cid": HEDGE_PREFIX + hashlib.sha256(fill["id"].encode()).hexdigest()[:40],
                "side": hedge_side, "size": qty, "price": target,
                "unknown": False, "retry_at": 0, "attempts": 0,
            }
            log.info("%s 主单成交 %s %s @ %s", state.market["id"], fill["side"], qty, price)

    async def send_hedges(self, state):
        client = self.clients[state.symbol]
        for fill_id, hedge in list(state.hedges.items()):
            if time.monotonic() < hedge["retry_at"]:
                continue
            try:
                if hedge["unknown"]:
                    order = await self.rest(
                        client, "fetch_order", "client:" + hedge["cid"], state.symbol, priority=1,
                    )
                else:
                    order = await self.rest(
                        client, "create_order", state.symbol, "limit",
                        hedge["side"], hedge["size"], hedge["price"],
                        {"clientOrderId": hedge["cid"], "postOnly": False}, priority=1,
                    )
            except OndoFatalError as exc:
                hedge["unknown"] = True
                hedge["retry_at"] = time.monotonic() + RECONCILE_SECONDS
                state.fatal = str(exc)
                self.exit_code = 1
                log.critical("%s 对冲结果待确认，只查单不重发: %s", state.market["id"], hedge["cid"])
                return
            except OndoAPIError as exc:
                if exc.uncertain:
                    hedge["unknown"] = True
                hedge["attempts"] += 1
                hedge["retry_at"] = max(
                    self.limiter.cooldown_until,
                    time.monotonic() + min(10, 2 ** min(hedge["attempts"], 4)),
                )
                if exc.status != 429:
                    log.error("%s 对冲待重试 %s: %s", state.market["id"], hedge["cid"], exc)
                return
            if (
                order["clientOrderId"] != hedge["cid"]
                or order["side"] != hedge["side"]
                or Decimal(order["info"]["size"]) != hedge["size"]
                or Decimal(order["info"]["price"]) != hedge["price"]
            ):
                raise OndoFatalError(f"对冲单内容不一致: {hedge['cid']}")
            del state.hedges[fill_id]
            log.info("%s 反向单 %s %s @ %s id=%s",
                     state.market["id"], hedge["side"], hedge["size"],
                     hedge["price"], order["id"])

    async def sync_main(self, state, entry, cancel=False, priority=0):
        client = self.clients[state.symbol]
        order_id = entry["id"] or "client:" + entry["cid"]
        must_cancel = (
            cancel or self.closing or bool(self.tripped) or self.recovering or not self.risk_ok
            or not self.ws_ready or bool(state.stopped or state.fatal)
        )
        if entry["status"] in TERMINAL and entry.get("raw"):
            order = client._parse_order(entry["raw"])
        elif must_cancel:
            order = await self.rest(client, "cancel_order", order_id, state.symbol, priority=priority)
        else:
            order = await self.rest(client, "fetch_order", order_id, state.symbol, priority=1)
        self.observe_order(state, order["info"])
        if (
            not cancel and entry["status"] not in TERMINAL
            and (self.closing or self.tripped or self.recovering or not self.risk_ok or not self.ws_ready
                 or state.stopped or state.fatal)
        ):
            order = await self.rest(client, "cancel_order", order["id"], state.symbol, priority=0)
            self.observe_order(state, order["info"])
        # 只补查尚未收到的成交；零成交订单和已由 WS 补齐的订单不必查明细。
        if entry["seen_size"] < entry["filled"]:
            fills = await self.rest(client, "fetch_order_fills", order["id"], state.symbol, priority=1)
            self.accept_fills(state, fills)
        # 终态累计成交与逐笔成交对齐后，才能释放主单槽位。
        if entry["seen_size"] < entry["filled"]:
            raise OndoAPIError(f"成交明细尚未齐全: {entry['cid']}")
        if entry["status"] in TERMINAL:
            if entry["seen_size"] != entry["filled"]:
                raise OndoAPIError(f"终态成交数量尚未对齐: {entry['cid']}")
            entry["settled"] = True
            if state.main.get(entry["side"]) == entry["cid"]:
                del state.main[entry["side"]]
                state.partial.discard(entry["side"])
            state.tracked.pop(entry["cid"], None)
            state.by_id.pop(entry["id"], None)
            state.seen.difference_update(entry["fill_ids"])
            self.account_wake.set()
        elif cancel:
            raise OndoAPIError(f"撤单尚未确认: {entry['cid']}")

    async def cancel_main(self, state, sides=("buy", "sell"), priority=0):
        # 只按已登记的主单 ID 撤单；永远不调用整币或全账户撤单。
        for side in sides:
            cid = state.main.get(side)
            if cid:
                entry = state.tracked[cid]
                if self.limiter.cooling or time.monotonic() < entry.get("retry_at", 0):
                    continue
                try:
                    await self.sync_main(state, entry, cancel=True, priority=priority)
                except OndoFatalError as exc:
                    state.fatal = str(exc)
                    self.exit_code = 1
                    log.critical("%s 主单状态异常: %s", state.market["id"], cid)
                except (OndoAPIError, ValueError, KeyError, TypeError, InvalidOperation) as exc:
                    entry["retry_at"] = max(time.monotonic() + 1, self.limiter.cooldown_until)
                    if not isinstance(exc, OndoAPIError) or exc.status != 429:
                        state.problem = str(exc)
                        self.need_reconcile = True
                        log.error("%s 主单撤销待确认 %s: %s", state.market["id"], cid, exc)

    async def refresh_positions(self, priority=3):
        versions = {s.symbol: s.position_seq for s in self.states.values()}
        positions = await self.rest(
            self.account_client, "fetch_positions", list(self.states), priority=priority,
            valid=lambda: not self.closing and self.ws_ready,
        )
        by_symbol = {p["symbol"]: p["info"] for p in positions}
        for state in self.states.values():
            if state.position_seq != versions[state.symbol]:
                continue
            raw = by_symbol.get(state.symbol)
            quantity = Decimal(raw["positionAmt"]) if raw else Decimal("0")
            mark = Decimal(str(raw["markPrice"])) if raw else state.mark
            if not quantity.is_finite() or not mark.is_finite() or (quantity and mark <= 0):
                raise ValueError("仓位或标记价格无效")
            state.position, state.mark = quantity, mark
            state.position_dirty = False
            state.position_seq += 1
            if abs(quantity) * mark > self.config.position_limit and not state.stopped:
                state.stopped = "单币仓位超过 myLimit"
                log.warning("%s %s: %s U", state.market["id"], state.stopped, abs(quantity) * mark)
            state.wake.set()

    async def reconcile(self, state=None):
        """全账户一次对账，结果分发给所有启用市场。"""
        async with self.reconcile_lock:
            if self.closing or self.tripped or not self.ws_ready or self.limiter.cooling:
                return
            generation = self.generation
            cooldown_generation = self.limiter.cooldown_generation
            priority = 1 if self.recovering else 3
            await self.refresh_positions(priority)
            started = time.monotonic()
            orders = await self.rest(
                self.account_client, "fetch_open_orders", priority=priority,
                valid=lambda: not self.closing and self.ws_ready,
            )
            open_ids = {o["id"] for o in orders}
            for order in orders:
                current = self.by_market.get(order["info"]["market"])
                if current:
                    self.observe_order(current, order["info"])
            if (
                generation != self.generation or not self.ws_ready or self.limiter.cooling
                or cooldown_generation != self.limiter.cooldown_generation
            ):
                return
            for current in self.states.values():
                for entry in current.tracked.values():
                    if (
                        entry["id"] and entry["id"] not in open_ids
                        and entry.get("accepted_at", float("inf")) <= started
                        and entry["status"] not in TERMINAL
                    ):
                        entry["needs_lookup"] = True
                current.synced_generation = generation
                current.problem = ""
                current.wake.set()
            self.need_reconcile = False
            self.recovering = False

    async def account_loop(self):
        next_sync = 0
        retry_at = 0
        while not self.closing:
            self.account_wake.clear()
            now = time.monotonic()
            if (
                self.ws_ready and not self.tripped and not self.limiter.cooling
                and now >= retry_at
            ):
                try:
                    # 429 后先让各币撤主单，再恢复账户快照。
                    if self.recovering and any(s.main for s in self.states.values()):
                        pass
                    elif self.need_reconcile or now >= next_sync:
                        await self.reconcile()
                        next_sync = time.monotonic() + RECONCILE_SECONDS
                    elif any(s.position_dirty and now >= s.position_refresh_at
                             for s in self.states.values()):
                        await self.refresh_positions(priority=1)
                    for state in self.states.values():
                        if (
                            self.recovering or state.stopped or state.fatal
                            or time.monotonic() - state.book_at < BOOK_SECONDS
                            or time.monotonic() < state.book_retry_at
                        ):
                            continue
                        state.book_refreshing = True
                        started = time.monotonic()
                        try:
                            book = await self.rest(
                                self.account_client, "fetch_order_book", state.symbol,
                                priority=3,
                                valid=lambda s=state: (
                                    not self.closing and self.ws_ready and not self.recovering
                                    and time.monotonic() - s.book_at >= BOOK_SECONDS
                                ),
                            )
                            if state.book_at <= started:
                                self.set_book(state, book["info"])
                        except OndoRequestSkipped:
                            pass
                        finally:
                            state.book_refreshing = False
                            state.book_retry_at = time.monotonic() + BOOK_SECONDS
                            state.wake.set()
                except OndoRequestSkipped:
                    pass
                except (OndoAPIError, ValueError, KeyError, TypeError, InvalidOperation) as exc:
                    self.need_reconcile = True
                    retry_at = max(time.monotonic() + 2, self.limiter.cooldown_until)
                    if not isinstance(exc, OndoAPIError) or exc.status != 429:
                        log.error("全账户对账失败，暂停主单: %s", exc)
                        for state in self.states.values():
                            state.problem = str(exc)
                            state.wake.set()
                except Exception:
                    log.exception("账户对账异常，开始退出清理")
                    self.exit_code = 1
                    self.request_stop()
            try:
                await asyncio.wait_for(self.account_wake.wait(), timeout=0.25)
            except asyncio.TimeoutError:
                pass

    async def place_quote(self, state, side):
        if not self.quote_allowed(state) or time.monotonic() - state.book_at >= BOOK_SECONDS:
            return
        if time.monotonic() < state.retry_at.get(side, 0):
            return
        client = self.clients[state.symbol]
        mid = sum(state.book) / 2
        step = Decimal(state.market["precision"]["price"])
        direction = -1 if side == "buy" else 1
        target_ticks = mid * (1 + direction * TARGET / BPS) / step
        prices = {
            target_ticks.to_integral_value(rounding=ROUND_FLOOR) * step,
            target_ticks.to_integral_value(rounding=ROUND_CEILING) * step,
        }
        candidates = []
        for price in prices:
            distance = direction * (price - mid) / mid * BPS
            if price > 0 and self.config.near_limit <= distance <= self.config.far_limit:
                candidates.append((abs(distance - TARGET), -distance, price))
        if not candidates:
            state.retry_at[side] = time.monotonic() + 1
            log.debug("%s %s 没有落在 %s~%sbps 内的合法 tick",
                      state.market["id"], side, self.config.near_limit, self.config.far_limit)
            return
        price = min(candidates)[2]
        size = Decimal(client.amount_to_precision(state.symbol, self.config.order_size / price))
        if size <= 0:
            raise ValueError(f"{state.market['id']} orderSize 小于最小数量")
        cid = MAIN_PREFIX + uuid.uuid4().hex
        entry = {
            "cid": cid, "id": None, "side": side, "price": price, "size": size,
            "filled": Decimal("0"), "seen_size": Decimal("0"),
            "status": "pending", "settled": False, "uncertain": True, "fill_ids": set(),
            "created_at": time.monotonic(), "needs_lookup": False,
        }
        # 网络请求前登记，风控触发时也能按客户端 ID 找回这张单。
        state.tracked[cid] = entry
        state.main[side] = cid
        try:
            def still_valid():
                if not self.quote_allowed(state) or time.monotonic() - state.book_at >= BOOK_SECONDS:
                    return False
                latest_mid = sum(state.book) / 2
                distance = direction * (price - latest_mid) / latest_mid * BPS
                return self.config.near_limit <= distance <= self.config.far_limit
            order = await self.rest(
                client, "create_order", state.symbol, "limit", side, size, price,
                {"clientOrderId": cid, "postOnly": True},
                priority=2, post_valid=still_valid,
            )
        except OndoRequestSkipped:
            del state.main[side]
            del state.tracked[cid]
            return
        except OndoFatalError as exc:
            state.fatal = str(exc)
            self.exit_code = 1
            log.critical("%s 主单待确认: %s", state.market["id"], cid)
            return
        except OndoAPIError as exc:
            # 明确拒单才允许释放槽位；未确认的订单仍保留以便撤销。
            if exc.uncertain:
                entry["needs_lookup"] = True
                state.problem = f"主单结果待确认: {cid}"
                self.need_reconcile = True
                self.account_wake.set()
                return
            del state.main[side]
            del state.tracked[cid]
            state.retry_at[side] = max(time.monotonic() + 1, self.limiter.cooldown_until)
            if exc.code == "post_only_has_match":
                log.debug("%s %s Post-only 拒单，等待新盘口", state.market["id"], side)
                return
            raise
        self.observe_order(state, order["info"])
        log.info("%s 主单 %s %s @ %s id=%s",
                 state.market["id"], side, size, price, order["id"])
        if self.closing or self.tripped or not self.risk_ok or not self.ws_ready:
            await self.cancel_main(state)

    async def run_market(self, state):
        while True:
            state.wake.clear()
            try:
                while state.updates:
                    self.observe_order(state, state.updates.popleft())
                fills = list(state.fills)
                state.fills.clear()
                self.accept_fills(state, fills)
                if not self.limiter.cooling:
                    if self.primary_paused(state):
                        await self.cancel_main(state)
                    await self.send_hedges(state)
                    if state.partial:
                        await self.cancel_main(state, tuple(state.partial))
                        await self.send_hedges(state)
                    for entry in list(state.tracked.values()):
                        if time.monotonic() < entry.get("retry_at", 0):
                            continue
                        if entry["status"] in TERMINAL or entry.get("needs_lookup"):
                            await self.sync_main(state, entry)
                            await self.send_hedges(state)
                    if self.quote_allowed(state) and time.monotonic() - state.book_at < BOOK_SECONDS:
                        for side in ("buy", "sell"):
                            if not self.quote_allowed(state):
                                break
                            cid = state.main.get(side)
                            if cid:
                                entry = state.tracked[cid]
                                mid = sum(state.book) / 2
                                direction = -1 if side == "buy" else 1
                                distance = direction * (entry["price"] - mid) / mid * BPS
                                if not self.config.near_limit <= distance <= self.config.far_limit:
                                    priority = 0 if distance < self.config.near_limit else 2
                                    await self.cancel_main(state, (side,), priority=priority)
                                    await self.send_hedges(state)
                            if side not in state.main:
                                await self.place_quote(state, side)
                    if time.monotonic() - state.book_at >= BOOK_SECONDS or state.position_dirty:
                        self.account_wake.set()
            except OndoRequestSkipped:
                pass
            except OndoFatalError as exc:
                state.fatal = str(exc)
                self.exit_code = 1
                log.critical("%s 停挂: %s", state.market["id"], exc)
            except (OndoAPIError, ValueError, KeyError, TypeError, InvalidOperation) as exc:
                if not isinstance(exc, OndoAPIError) or exc.status != 429:
                    state.problem = str(exc)
                    state.synced_generation = -1
                    self.need_reconcile = True
                    log.error("%s 暂停主单: %s", state.market["id"], exc)
                    for entry in state.tracked.values():
                        entry["retry_at"] = time.monotonic() + 1
                    self.account_wake.set()
                    await self.cancel_main(state)
            except Exception:
                log.exception("%s 程序异常，开始退出清理", state.market["id"])
                self.exit_code = 1
                self.request_stop()
            if self.closing:
                if not state.main and not state.hedges and not state.fills and not state.updates:
                    break
                if time.monotonic() >= self.shutdown_deadline:
                    self.exit_code = 1
                    log.critical("%s 退出清理未完成，主单=%s，待对冲=%s",
                                 state.market["id"], list(state.main.values()),
                                 [h["cid"] for h in state.hedges.values()])
                    break
            try:
                await asyncio.wait_for(state.wake.wait(), timeout=0.25)
            except asyncio.TimeoutError:
                pass

    async def risk_loop(self):
        pending = None
        next_query = 0
        next_display = time.monotonic() + RISK_SECONDS
        try:
            while not self.closing:
                now = time.monotonic()
                show = now >= next_display
                if pending is not None and pending.done():
                    try:
                        risk = pending.result()
                        first = self.last_risk is None
                        self.last_risk, self.risk_at = risk, now
                        self.risk_ok = True
                        reasons = []
                        if risk["marginRatio"] > self.config.mmr:
                            reasons.append("保证金率超过 MMR")
                        if risk["leverage"] > self.config.max_leverage:
                            reasons.append("总杠杆超过 leverage")
                        if reasons and not self.tripped:
                            self.tripped = "；".join(reasons)
                            log.critical("全账户停止主挂单: %s", self.tripped)
                        show = show or first
                    except OndoRequestSkipped:
                        pass
                    except Exception as exc:
                        self.risk_ok = False
                        if not isinstance(exc, OndoAPIError) or exc.status != 429:
                            log.error("风控查询失败，暂停主单: %s", exc)
                        next_query = max(next_query, now + 2)
                    finally:
                        pending = None
                        for state in self.states.values():
                            state.wake.set()
                if self.risk_at and now - self.risk_at > 2 * RISK_SECONDS:
                    self.risk_ok = False
                if (
                    pending is None and not self.limiter.cooling
                    and (now >= next_query or self.risk_refresh)
                ):
                    self.risk_refresh = False
                    next_query = now + RISK_SECONDS
                    pending = asyncio.create_task(self.rest(
                        self.risk_client, "fetch_account_risk", priority=0,
                        valid=lambda: not self.closing,
                    ))
                if show:
                    if self.last_risk:
                        ratio = f"{self.last_risk['marginRatio']:.4%}/{self.config.mmr:.2%}"
                        leverage = f"{self.last_risk['leverage']:.4f}x/{self.config.max_leverage}x"
                        age = f"{now - self.risk_at:.1f}s"
                    else:
                        ratio = leverage = "等待查询"
                        age = "无数据"
                    status = ("永久停挂" if self.tripped else
                              "冷却/恢复中" if self.recovering or self.limiter.cooling else
                              "查询异常/等待" if not self.risk_ok else "监控中")
                    print(
                        f"{datetime.now():%Y-%m-%d %H:%M:%S} | 保证金率={ratio} | "
                        f"总杠杆={leverage} | 数据年龄={age} | rps={self.config.rps} | "
                        f"主单={status}", flush=True,
                    )
                    next_display = now + RISK_SECONDS
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=0.05)
                except asyncio.TimeoutError:
                    pass
        finally:
            if pending is not None:
                await asyncio.gather(pending, return_exceptions=True)

    async def feed_loop(self):
        while not self.closing:
            self.ws_ready = False
            self.generation += 1
            for state in self.states.values():
                state.book_at = 0
                state.synced_generation = -1
                state.wake.set()
            try:
                async for message in self.risk_client.stream(self.by_market):
                    if message["type"] == "ready":
                        self.ws_ready = True
                        self.need_reconcile = True
                        self.account_wake.set()
                        log.info("实时行情与私有成交订阅已就绪")
                        for state in self.states.values():
                            state.wake.set()
                        continue
                    channel = message.get("channel")
                    data = message.get("data", [])
                    rows = data if isinstance(data, list) else [data]
                    for raw in rows:
                        state = self.by_market.get(raw.get("market"))
                        if state is None:
                            continue
                        if channel == "topOfBooksPerps":
                            self.set_book(state, raw)
                        elif channel == "fillsPerps":
                            state.position_seq += 1
                            raw = {**raw, "_position_seq": state.position_seq}
                            state.fills.append(raw)
                            state.position_dirty = True
                            state.position_refresh_at = time.monotonic() + 1
                            self.account_wake.set()
                        elif channel == "ordersPerps":
                            state.updates.append(raw)
                        elif channel == "markPricesPerps":
                            mark = Decimal(str(raw["markPrice"]))
                            if not mark.is_finite() or mark <= 0:
                                raise ValueError("无效标记价格")
                            state.mark = mark
                            if (
                                not state.position_dirty
                                and abs(state.position) * mark > self.config.position_limit
                                and not state.stopped
                            ):
                                state.stopped = "单币仓位超过 myLimit"
                                log.warning("%s %s", state.market["id"], state.stopped)
                        elif channel == "positionsPerps":
                            quantity = abs(Decimal(str(raw["netQuantity"])))
                            mark = Decimal(str(raw["markPrice"]))
                            if (
                                not quantity.is_finite() or not mark.is_finite()
                                or (quantity > 0 and mark <= 0)
                            ):
                                raise ValueError("无效仓位推送")
                            if quantity * mark > self.config.position_limit and not state.stopped:
                                state.stopped = "单币仓位超过 myLimit"
                                log.warning("%s %s", state.market["id"], state.stopped)
                            if raw["direction"] not in {"long", "short", "neutral"}:
                                raise ValueError("无效仓位方向")
                            state.position = -quantity if raw["direction"] == "short" else quantity
                            state.mark = mark
                            state.position_seq += 1
                            state.position_dirty = False
                        state.wake.set()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("实时连接中断，暂停主单并重连: %s", type(exc).__name__)
            finally:
                self.ws_ready = False
                self.need_reconcile = True
                self.account_wake.set()
                for state in self.states.values():
                    state.synced_generation = -1
                    state.wake.set()
            if not self.closing:
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=2)
                except asyncio.TimeoutError:
                    pass

    async def run(self):
        # 仅内存模式：不接管遗留主单，也不自动补做上次进程的成交。
        while True:
            try:
                orders = await self.rest(self.account_client, "fetch_open_orders", priority=0)
                break
            except OndoAPIError as exc:
                if exc.status != 429:
                    raise
        old = [(o["symbol"], o["clientOrderId"]) for o in orders
               if (o.get("clientOrderId") or "").startswith(MAIN_PREFIX)]
        if old:
            raise RuntimeError(f"存在遗留主单（含未启用市场），请人工核对后重启: {old}")
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop)
            except NotImplementedError:
                signal.signal(sig, lambda *_: loop.call_soon_threadsafe(self.request_stop))
        feed = asyncio.create_task(self.feed_loop(), name="ondo-feed")
        workers = [asyncio.create_task(self.run_market(s), name=s.market["id"])
                   for s in self.states.values()]
        risk = asyncio.create_task(self.risk_loop(), name="ondo-risk")
        account = asyncio.create_task(self.account_loop(), name="ondo-account")
        try:
            await self.stop_event.wait()
        finally:
            self.request_stop()
            feed.cancel()
            await asyncio.gather(feed, return_exceptions=True)
            # 不取消进行中的 REST 写请求；等返回后按登记的客户端 ID 清理。
            results = await asyncio.gather(*workers, risk, account, return_exceptions=True)
            if any(isinstance(result, BaseException) for result in results):
                self.exit_code = 1
        return self.exit_code


async def main(config):
    limiter = RequestLimiter(config.rps)
    risk_client = OndoExchange.from_env(limiter)
    account_client = None
    clients = {}
    try:
        while True:
            try:
                await asyncio.to_thread(risk_client.load_markets)
                break
            except OndoAPIError as exc:
                if exc.status != 429:
                    raise
                log.warning("市场初始化被限流，等待共享冷却后重试")
        symbols = [risk_client.market(name)["symbol"] for name in config.symbols]
        if len(set(symbols)) != len(symbols):
            raise ValueError("symbol 映射到了相同市场")
        account_client = OndoExchange.from_env(limiter)
        account_client._markets = risk_client._markets
        account_client._markets_by_id = risk_client._markets_by_id
        for symbol in symbols:
            client = OndoExchange.from_env(limiter)
            # 各币有独立 HTTP 会话；市场元数据只读共享。
            client._markets = risk_client._markets
            client._markets_by_id = risk_client._markets_by_id
            clients[symbol] = client
        return await PostOnlyBot(config, risk_client, clients, account_client).run()
    finally:
        limiter.stop()
        for client in clients.values():
            client.close()
        if account_client:
            account_client.close()
        risk_client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ondo 双边 Post-only 奖励挂单")
    parser.add_argument("--check", action="store_true", help="只检查本地配置，不连接交易所或下单")
    args = parser.parse_args()
    try:
        config = Config.from_env()
        logging.basicConfig(
            level=os.getenv("LOG_LEVEL", "INFO").upper(),
            format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout,
        )
        if args.check:
            print(f"配置有效: {', '.join(config.symbols)} | orderSize={config.order_size} | "
                  f"myLimit={config.position_limit} | verse={config.verse}bps | "
                  f"nearLimit={config.near_limit}bps | farLimit={config.far_limit}bps | rps={config.rps} | "
                  f"MMR={config.mmr:.2%} | leverage={config.max_leverage}x")
        else:
            sys.exit(asyncio.run(main(config)))
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as exc:
        print(f"启动失败: {exc}", file=sys.stderr)
        sys.exit(1)
