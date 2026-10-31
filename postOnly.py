"""Ondo v2.0：跟随最大外部价位；--check 只检查本地配置。"""

import argparse
import asyncio
import logging
import os
import re
import signal
import sys
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from dotenv import load_dotenv
from ondo_exchange import AccountLimiter, OndoAPIError, OndoExchange, OndoFatalError, OndoRequestSkipped

BPS = Decimal('10000')
BAND = Decimal('5')
RISK_SECONDS = 10
RECONCILE_SECONDS = 30
BOOK_SECONDS = 3
MAIN_PREFIX = 'po_q_'
TERMINAL = {'filled', 'canceled'}
log = logging.getLogger('postOnly')


@dataclass
class MarketConfig:
    symbol: str
    order_size: Decimal
    position_limit: Decimal


@dataclass
class Config:
    markets: tuple
    mmr: Decimal = Decimal('0.2')
    max_leverage: Decimal = Decimal('5')

    @property
    def symbols(self):
        return tuple(m.symbol for m in self.markets)

    @classmethod
    def from_env(cls):
        load_dotenv(Path(__file__).with_name('.env'), encoding='utf-8-sig')
        values = {}
        markets = []
        for i in (1, 2, 3):
            symbol = os.getenv(f'symbol{i}', '').strip().upper()
            if not symbol:
                continue
            for name in (f'orderSize{i}', f'myLimit{i}'):
                values[name] = os.getenv(name, '').strip()
            markets.append((symbol, f'orderSize{i}', f'myLimit{i}'))
        if not markets or len({m[0] for m in markets}) != len(markets):
            raise ValueError('至少填写一个 symbol，非空市场不能重复')
        values.update(MMR=os.getenv('MMR', '0.2').strip(), leverage=os.getenv('leverage', '5').strip())
        for name, raw in values.items():
            try:
                number = Decimal(raw)
            except InvalidOperation:
                raise ValueError(f'{name} 必须是有效数字') from None
            if not number.is_finite() or number <= 0:
                raise ValueError(f'{name} 必须是有限正数')
            values[name] = number
        return cls(tuple(MarketConfig(s, values[o], values[l]) for s, o, l in markets),
                   values['MMR'], values['leverage'])


@dataclass
class MarketState:
    market: dict
    order_size: Decimal
    position_limit: Decimal
    main: dict = field(default_factory=dict)
    tracked: dict = field(default_factory=dict)
    by_id: dict = field(default_factory=dict)
    own_orders: dict = field(default_factory=dict)
    orders_seq: int = 0
    fills: deque = field(default_factory=deque)
    updates: deque = field(default_factory=deque)
    seen: set = field(default_factory=set)
    partial: set = field(default_factory=set)
    retry_at: dict = field(default_factory=dict)
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    book: tuple = ()
    book_at: float = 0
    book_time: float = 0
    depth: tuple = ()
    depth_at: float = 0
    depth_time: float = 0
    depth_seq: int = 0
    depth_barrier: int = -1
    position: Decimal = Decimal('0')
    mark: Decimal = Decimal('0')
    position_dirty: bool = True
    position_seq: int = 0
    position_refresh_at: float = 0
    synced_generation: int = -1
    stopped: str = ''
    fatal: str = ''
    problem: str = ''

    @property
    def symbol(self):
        return self.market['symbol']


class PostOnlyBot:
    def __init__(self, config, risk_client, clients, account_client=None):
        self.config = config
        self.risk_client = risk_client
        self.clients = clients
        self.account_client = account_client or risk_client
        self.limiter = AccountLimiter()
        for client in {risk_client, self.account_client, *clients.values()}:
            client.limiter = self.limiter
        settings = {risk_client.market(m.symbol)['symbol']: m for m in config.markets}
        self.states = {s: MarketState(c.market(s), settings[s].order_size, settings[s].position_limit)
                       for s, c in clients.items()}
        self.by_market = {s.market['id']: s for s in self.states.values()}
        self.risk_ok = self.ws_ready = self.closing = False
        self.tripped = ''
        self.generation = self.exit_code = 0
        self.shutdown_deadline = 0
        self.stop_event = asyncio.Event()
        self.account_wake = asyncio.Event()
        self.need_reconcile = self.risk_refresh = True
        self.recovering = False
        self.reconcile_lock = asyncio.Lock()
        self.last_risk = None
        self.risk_at = 0

    def primary_paused(self, state):
        return (self.closing or bool(self.tripped) or not self.risk_ok or self.recovering
                or self.limiter.cooling or not self.ws_ready
                or state.synced_generation != self.generation
                or bool(state.stopped or state.fatal or state.problem))

    def quote_allowed(self, state):
        return (not self.primary_paused(state) and not state.position_dirty
                and not state.partial and not state.fills and not state.updates
                and not any(e['id'] and e['status'] == 'pending' for e in state.tracked.values()))

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
            if exc.status == 429 or exc.code == 'too_many_requests':
                self.recovering = self.need_reconcile = self.risk_refresh = True
                self.risk_ok = False
                self.account_wake.set()
                for current in self.states.values():
                    current.wake.set()
            raise

    def request_stop(self):
        if not self.closing:
            self.closing = True
            self.shutdown_deadline = time.monotonic() + 60
            self.stop_event.set()
            self.account_wake.set()
            for bucket in self.limiter.buckets.values():
                with bucket.condition:
                    bucket.condition.notify_all()
            for state in self.states.values():
                state.wake.set()

    def set_book(self, state, raw, depth=True):
        if raw.get('market', state.market['id']) != state.market['id']:
            raise ValueError('盘口市场不一致')
        # RFC3339Nano 的小数秒可有 1~9 位；Python 3.10 仅接受 3/6 位。
        timestamp = raw.get('time')
        match = re.fullmatch(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})',
                             timestamp) if isinstance(timestamp, str) else None
        if not match:
            raise ValueError('盘口时间格式无效，需要带时区的 RFC3339 时间')
        fraction = (match[2] or '').ljust(6, '0')[:6]
        zone = '+00:00' if match[3] == 'Z' else match[3]
        stamp = datetime.fromisoformat(f'{match[1]}.{fraction}{zone}').timestamp()
        if time.time() - stamp >= BOOK_SECONDS:
            return
        if stamp - time.time() > BOOK_SECONDS:
            raise ValueError('盘口时间超前，请检查系统时钟')
        if stamp <= (state.depth_time if depth else state.book_time):
            return
        tick = Decimal(state.market['precision']['price'])
        if depth and raw.get('depthLevels') is not None and Decimal(raw['depthLevels']) != tick:
            raise ValueError('盘口聚合粒度与 tick 不一致')
        levels = []
        for side in ('bids', 'asks'):
            book = {}
            for p, q in raw[side]:
                price, qty = Decimal(str(p)), Decimal(str(q))
                if (not price.is_finite() or not qty.is_finite() or price <= 0 or qty < 0
                    or price % tick):
                    raise ValueError('盘口价格或数量无效')
                if qty:
                    book[price] = book.get(price, Decimal(0)) + qty
            levels.append(book)
        if not all(levels) or max(levels[0]) >= min(levels[1]):
            raise ValueError('盘口缺失或交叉')
        if depth:
            state.depth = tuple(levels)
            state.depth_time = stamp
            state.depth_at = time.monotonic()
            state.depth_seq += 1
        if stamp >= state.book_time:
            state.book = (max(levels[0]), min(levels[1]))
            state.book_time = stamp
            state.book_at = time.monotonic()

    def quote_targets(self, state):
        targets = {'buy': None, 'sell': None}
        if not state.depth or not state.book or time.monotonic() - state.depth_at >= BOOK_SECONDS:
            return targets
        external = [dict(levels) for levels in state.depth]
        for order in list(state.own_orders.values()):
            # 未触发的条件单尚未进入盘口，不能从公开深度中扣除。
            if order['status'] != 'open':
                continue
            index = 0 if order['side'] == 'buy' else 1
            remaining = Decimal(order['size']) - Decimal(order['filledSize'])
            price = Decimal(order['price'])
            if remaining > external[index].get(price, Decimal(0)):
                raise ValueError('本人订单与深度快照不同步，等待对账')
            if remaining:
                external[index][price] -= remaining
        bid, ask = state.book
        mid = (bid + ask) / 2
        tick = Decimal(state.market['precision']['price'])
        for index, side in enumerate(('buy', 'sell')):
            direction = -1 if side == 'buy' else 1
            eligible = [(p * q, abs(p - mid), p) for p, q in external[index].items()
                        if q > 0 and 0 <= direction * (p - mid) / mid * BPS <= BAND]
            if not eligible:
                continue
            wall = max(eligible)[2]
            price = wall + direction * tick
            distance = direction * (price - mid) / mid * BPS
            if price > 0 and 0 < distance <= BAND and (price < bid if side == 'buy' else price > ask):
                targets[side] = price
        return targets

    def observe_order(self, state, raw):
        oid = str(raw['orderId'])
        previous = state.own_orders.get(oid)
        if raw['status'] in {'fullyfilled', 'canceled'}:
            state.own_orders.pop(oid, None)
        else:
            state.own_orders[oid] = dict(raw)
        if previous != state.own_orders.get(oid):
            state.orders_seq += 1
            state.depth_barrier = state.depth_seq
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
            or size != entry["size"] or Decimal(raw['price']) != entry['price']
            or not 0 <= filled <= size
        ):
            raise OndoFatalError(f"主单数量异常: {cid}")
        entry["id"] = order_id
        entry.setdefault("accepted_at", time.monotonic())
        entry["uncertain"] = False
        state.by_id[order_id] = cid
        status = "filled" if raw["status"] == "fullyfilled" else raw["status"]
        if status not in {"open", "pending", "untriggered", *TERMINAL}:
            raise OndoFatalError(f"未知订单状态: {status}")
        if filled > entry['filled']:
            state.position_dirty = True
            state.position_refresh_at = time.monotonic() + 1
            state.position_seq += 1
            self.account_wake.set()
        if filled >= entry["filled"]:
            entry["filled"] = filled
            if entry["status"] not in TERMINAL or status in TERMINAL:
                entry["status"] = status
            entry["raw"] = raw
        entry["needs_lookup"] = status == 'pending'
        if status == 'pending':
            entry['retry_at'] = time.monotonic() + 1
        if filled > 0 and not entry["settled"]:
            state.partial.add(entry["side"])

    def accept_fills(self, state, fills):
        """逐笔成交去重；净仓位只采用交易所快照，不生成反向单。"""
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
            state.depth_barrier = state.depth_seq
            log.info('%s 主单成交 %s %s @ %s', state.market['id'], fill['side'], qty, price)

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
            state.depth_barrier = state.depth_seq
            self.account_wake.set()
        elif cancel:
            raise OndoAPIError(f"撤单尚未确认: {entry['cid']}")

    async def cancel_main(self, state, sides=('buy', 'sell'), priority=0):
        entries = [state.tracked[state.main[s]] for s in sides if s in state.main]
        entries = [e for e in entries if time.monotonic() >= e.get('retry_at', 0)]
        known = [e for e in entries if e['id'] and e['status'] not in TERMINAL]
        if known and not self.limiter.buckets['cancel'].cooling:
            try:
                orders = await self.rest(self.clients[state.symbol], 'cancel_orders', state.symbol,
                                         [e['id'] for e in known], priority=priority)
                for order in orders:
                    self.observe_order(state, order['info'])
            except (OndoAPIError, ValueError, KeyError, TypeError, InvalidOperation) as exc:
                for entry in known:
                    entry['retry_at'] = time.monotonic() + 1
                state.problem = str(exc)
                self.need_reconcile = True
                log.warning('%s 批撤待确认: %s', state.market['id'], exc)
        for entry in entries:
            if time.monotonic() < entry.get('retry_at', 0):
                continue
            try:
                # 已批撤的非终态不重复 DELETE；下轮仍保留槽位并重试。
                await self.sync_main(state, entry, cancel=entry not in known, priority=priority)
                if not entry['settled']:
                    entry['retry_at'] = time.monotonic() + 1
            except (OndoAPIError, OndoFatalError, ValueError, KeyError, TypeError, InvalidOperation) as exc:
                entry['retry_at'] = time.monotonic() + 1
                state.problem = str(exc)
                self.need_reconcile = True
                log.warning('%s 撤单或成交待确认: %s', state.market['id'], exc)

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
            if abs(quantity) * mark > state.position_limit and not state.stopped:
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
            versions = {s.symbol: s.orders_seq for s in self.states.values()}
            own = {s.symbol: {} for s in self.states.values()}
            orders = await self.rest(
                self.account_client, "fetch_open_orders", priority=priority,
                valid=lambda: not self.closing and self.ws_ready,
            )
            open_ids = {o["id"] for o in orders}
            for order in orders:
                current = self.by_market.get(order["info"]["market"])
                if current:
                    own[current.symbol][order['id']] = order['info']
            if (
                generation != self.generation or not self.ws_ready or self.limiter.cooling
                or cooldown_generation != self.limiter.cooldown_generation
            ):
                return
            changed = False
            for current in self.states.values():
                if current.orders_seq != versions[current.symbol]:
                    changed = True
                    continue
                if current.own_orders != own[current.symbol]:
                    current.depth_barrier = current.depth_seq
                current.own_orders = own[current.symbol]
                current.orders_seq += 1
                for raw in list(current.own_orders.values()):
                    self.observe_order(current, raw)
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
            self.need_reconcile = changed
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

    async def place_quotes(self, state, sides=('buy', 'sell')):
        if (not self.quote_allowed(state) or state.depth_seq <= state.depth_barrier
            or time.monotonic() - state.book_at >= BOOK_SECONDS):
            return
        targets = self.quote_targets(state)
        entries = []
        for side in sides:
            price = targets[side]
            if side in state.main or price is None or time.monotonic() < state.retry_at.get(side, 0):
                continue
            size = Decimal(self.clients[state.symbol].amount_to_precision(state.symbol, state.order_size / price))
            if size <= 0:
                raise ValueError('orderSize 小于最小数量')
            cid = MAIN_PREFIX + uuid.uuid4().hex
            entry = {'cid': cid, 'id': None, 'side': side, 'price': price, 'size': size,
                     'filled': Decimal(0), 'seen_size': Decimal(0), 'status': 'pending',
                     'settled': False, 'uncertain': True, 'fill_ids': set(),
                     'created_at': time.monotonic(), 'needs_lookup': False}
            state.main[side] = cid
            state.tracked[cid] = entry
            entries.append(entry)
        if not entries:
            return
        def still_valid():
            try:
                if (not self.quote_allowed(state) or state.depth_seq <= state.depth_barrier
                    or time.monotonic() - state.book_at >= BOOK_SECONDS):
                    return False
                latest = self.quote_targets(state)
                return all(latest[e['side']] == e['price'] for e in entries)
            except (ValueError, KeyError, InvalidOperation):
                return False
        try:
            result = await self.rest(self.clients[state.symbol], 'create_orders', state.symbol,
                                     [{'clientOrderId': e['cid'], 'side': e['side'],
                                       'amount': e['size'], 'price': e['price']} for e in entries],
                                     priority=2, post_valid=still_valid)
        except OndoRequestSkipped:
            result = {'addedOrders': [], 'failedOrders': [
                {'order': {'clientOrderId': e['cid']}, 'errorCode': 'quote_expired'} for e in entries]}
        except OndoAPIError as exc:
            if exc.uncertain:
                raise OndoFatalError('批量下单结果不明，保留客户端 ID 等待撤销') from None
            result = {'addedOrders': [], 'failedOrders': [
                {'order': {'clientOrderId': e['cid']}, 'errorCode': exc.code} for e in entries]}
        for order in result['addedOrders']:
            self.observe_order(state, order['info'])
            log.info('%s 主单 %s %s @ %s', state.market['id'], order['side'], order['amount'], order['price'])
        for failure in result['failedOrders']:
            cid = failure['order']['clientOrderId']
            entry = state.tracked[cid]
            if failure.get('errorCode') == 'order_unconfirmed':
                entry['needs_lookup'] = True
                state.fatal = '下单结果待确认，只查单不重发'
                self.exit_code = 1
            else:
                state.main.pop(entry['side'], None)
                state.tracked.pop(cid, None)
                state.retry_at[entry['side']] = time.monotonic() + 1
                if failure.get('errorCode') not in {'quote_expired', 'post_only_has_match'}:
                    log.warning('%s 主单拒绝: %s', state.market['id'], failure.get('errorCode'))
        state.depth_barrier = state.depth_seq
        if self.primary_paused(state):
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
                stale = time.monotonic() - state.depth_at >= BOOK_SECONDS
                if self.primary_paused(state) or stale:
                    await self.cancel_main(state)
                else:
                    if state.partial:
                        await self.cancel_main(state, tuple(state.partial))
                    for entry in list(state.tracked.values()):
                        if ((entry['status'] in TERMINAL or entry.get('needs_lookup'))
                            and time.monotonic() >= entry.get('retry_at', 0)):
                            await self.sync_main(state, entry)
                    # 即使等待新深度，也不能让自己的主单停留在买一或卖一。
                    best = [side for side, cid in list(state.main.items()) if state.book and
                            (state.tracked[cid]['price'] >= state.book[0] if side == 'buy'
                             else state.tracked[cid]['price'] <= state.book[1])]
                    if best:
                        await self.cancel_main(state, tuple(best))
                    if self.quote_allowed(state) and state.depth_seq > state.depth_barrier:
                        targets = self.quote_targets(state)
                        changed = [side for side, cid in list(state.main.items())
                                   if state.tracked[cid]['price'] != targets[side]]
                        if changed:
                            await self.cancel_main(state, tuple(changed))
                        await self.place_quotes(state)
                if state.position_dirty:
                    self.account_wake.set()
            except OndoRequestSkipped:
                pass
            except OndoFatalError as exc:
                state.fatal = str(exc)
                self.exit_code = 1
                await self.cancel_main(state)
                log.critical('%s 停挂: %s', state.market['id'], exc)
            except (OndoAPIError, ValueError, KeyError, TypeError, InvalidOperation) as exc:
                state.problem = str(exc)
                self.need_reconcile = True
                self.account_wake.set()
                await self.cancel_main(state)
                log.warning('%s 暂停主单: %s', state.market['id'], exc)
            except Exception:
                log.exception('%s 程序异常，退出清理', state.market['id'])
                self.exit_code = 1
                self.request_stop()
            if self.closing:
                if not state.main and not state.fills and not state.updates:
                    break
                if time.monotonic() >= self.shutdown_deadline:
                    self.exit_code = 1
                    log.critical('%s 退出清理未完成: %s', state.market['id'], list(state.main.values()))
                    break
            try:
                await asyncio.wait_for(state.wake.wait(), timeout=0.1)
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
                              "连接/同步中" if not self.ws_ready or any(
                                  s.synced_generation != self.generation for s in self.states.values()) else
                              "冷却/恢复中" if self.recovering or self.limiter.cooling else
                              "查询异常/等待" if not self.risk_ok else "监控中")
                    print(
                        f"{datetime.now():%Y-%m-%d %H:%M:%S} | 保证金率={ratio} | "
                        f"总杠杆={leverage} | 数据年龄={age} | 下单/撤单=2/2 RPS | "
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
            channel, market = 'stream', '-'
            self.ws_ready = False
            self.generation += 1
            for state in self.states.values():
                state.book_at = state.depth_at = 0
                state.book_time = state.depth_time = 0
                state.depth_barrier = state.depth_seq
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
                    market = '-'
                    data = message.get("data", [])
                    rows = data if isinstance(data, list) else [data]
                    for raw in rows:
                        state = self.by_market.get(raw.get("market"))
                        if state is None:
                            continue
                        market = state.market['id']
                        if channel == "topOfBooksPerps":
                            self.set_book(state, raw, depth=False)
                        elif channel == 'depthBooksPerps':
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
                                and abs(state.position) * mark > state.position_limit
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
                            if quantity * mark > state.position_limit and not state.stopped:
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
                detail = str(exc)
                for name in ('api_key_id', '_api_secret'):
                    secret = getattr(self.risk_client, name, None)
                    if secret:
                        detail = detail.replace(secret, '<redacted>')
                trace = exc.__traceback__
                while trace and trace.tb_next:
                    trace = trace.tb_next
                source = (f'{Path(trace.tb_frame.f_code.co_filename).name}:{trace.tb_lineno}'
                          if trace else '-')
                log.warning('实时连接中断，暂停主单并重连: %s: %s | channel=%s market=%s source=%s',
                            type(exc).__name__, detail.replace('\n', ' ').replace('\r', ' ')[:500],
                            channel, market, source)
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
    limiter = AccountLimiter()
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
            for i, market in enumerate(config.markets, 1):
                print(f'{market.symbol}: orderSize={market.order_size} U/侧 | myLimit={market.position_limit} U')
            print(f'v2.0 | ±5bps | 深度WS/不限档 | 退后1 tick | Post-only | 下单/撤单=2/2 RPS | '
                  f'MMR={config.mmr:.2%} | leverage={config.max_leverage}x | 无反向单')
        else:
            sys.exit(asyncio.run(main(config)))
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as exc:
        print(f"启动失败: {exc}", file=sys.stderr)
        sys.exit(1)
