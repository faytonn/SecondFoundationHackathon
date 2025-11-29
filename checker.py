#!/usr/bin/env python3

from dataclasses import dataclass
from typing import List, Dict, Tuple
import sys
import re


# -------------------- Order book model -------------------- #

@dataclass
class Order:
    order_id: str
    side: str          # "buy" or "sell"
    price: int
    quantity: int
    timestamp: int     # sequence index for time priority
    user_id: str
    active: bool = True


# -------------------- Parsers -------------------- #

def parse_events(path: str) -> List[Dict]:
    """
    Parse events.sqml into a list of event dicts:
    {
        "type": "NEW" | "MODIFY" | "CANCEL",
        "timestamp": int,
        "order_id": str,
        "side": "buy"/"sell"/None,
        "price": int or None,
        "quantity": int or None,
        "user_id": str or None,
    }
    """

    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    def get(tag: str, block: str, default=None):
        # allow whitespace/newlines between [/ and tag]
        m = re.search(r"\[" + tag + r"\](.*?)\[/\s*" + tag + r"\]", block, flags=re.S)
        return m.group(1).strip() if m else default

    events_blocks: List[str] = []
    idx = 0
    start_tag = "[user-operation]"
    end_tag = "[/user-operation]"

    # manual scanning – robust when many blocks are on the same physical line
    while True:
        start = text.find(start_tag, idx)
        if start == -1:
            break
        end = text.find(end_tag, start)
        if end == -1:
            break
        end += len(end_tag)
        block = text[start:end]
        events_blocks.append(block)
        idx = end

    events: List[Dict] = []

    for block in events_blocks:
        op = get("op", block, "").strip()

        if op == "submit-order":
            etype = "NEW"
        elif op == "modify-order":
            etype = "MODIFY"
        elif op == "cancel-order":
            etype = "CANCEL"
        else:
            # ignore unknown ops just in case
            continue

        ts_str   = get("timestamp", block, "0")
        order_id = get("orderId", block, "")
        side     = get("side", block)
        user_id  = get("userId", block, None)

        if side:
            side = side.strip().lower()   # "Sell"/"Buy" → "sell"/"buy"

        price_str = get("price", block, None)
        qty_str   = get("quantity", block, None)

        price    = int(price_str) if price_str not in (None, "") else None
        quantity = int(qty_str)   if qty_str   not in (None, "") else None
        timestamp = int(ts_str) if ts_str else 0

        events.append({
            "type": etype,
            "timestamp": timestamp,
            "order_id": order_id,
            "side": side,
            "price": price,
            "quantity": quantity,
            "user_id": user_id,
        })

    print(f"Parsed {len(events)} events.")
    return events


def parse_trades(path: str) -> List[Dict]:
    """
    Parse trades.sqml into a list of trade dicts:
    {
        "id": str,
        "timestamp": int,
        "quantity": int,
        "price": int,
    }
    """
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    def get(tag: str, block: str, default=None):
        m = re.search(r"\[" + tag + r"\](.*?)\[/\s*" + tag + r"\]", block, flags=re.S)
        return m.group(1).strip() if m else default

    trades: List[Dict] = []

    for block in re.findall(r"\[trade\](.*?)\[/trade\]", text, flags=re.S):
        trade_id      = get("id", block, "")
        timestamp_str = get("timestamp", block, "0")
        quantity_str  = get("quantity", block, "0")
        price_str     = get("price", block, "0")

        timestamp = int(timestamp_str) if timestamp_str else 0
        quantity  = int(quantity_str)  if quantity_str else 0
        price     = int(price_str)     if price_str else 0

        trades.append({
            "id": trade_id,
            "timestamp": timestamp,
            "quantity": quantity,
            "price": price,
        })

    print(f"Parsed {len(trades)} trades.")
    return trades


# -------------------- Matching engine simulation -------------------- #

def simulate(events: List[Dict]) -> List[Tuple[int, int]]:
    """
    Re-simulate order book from events.
    Returns list of expected trades as (price, quantity) pairs
    in the order they occur.
    """

    bids: List[Order] = []   # best bid at index 0
    asks: List[Order] = []   # best ask at index 0
    orders_by_id: Dict[str, Order] = {}

    expected_trades: List[Tuple[int, int]] = []
    seq = 0  # sequence index for time priority

    def sort_books():
        # Price-time priority:
        #  - bids: highest price first, then earliest timestamp
        #  - asks: lowest price first, then earliest timestamp
        bids.sort(key=lambda o: (-o.price, o.timestamp))
        asks.sort(key=lambda o: (o.price, o.timestamp))

    def match_new_order(order: Order):
        nonlocal expected_trades
        book_own = bids if order.side == "buy" else asks
        book_opp = asks if order.side == "buy" else bids

        # Matching loop: always against best opposing order (book_opp[0])
        while order.active and order.quantity > 0 and book_opp:
            best = book_opp[0]

            # skip any zombie zero/negative-quantity resting orders
            if best.quantity <= 0 or not best.active:
                best.active = False
                book_opp.pop(0)
                continue

            # price crossing condition
            if order.side == "buy" and order.price < best.price:
                break
            if order.side == "sell" and order.price > best.price:
                break

            # trade occurs
            qty = min(order.quantity, best.quantity)
            trade_price = best.price  # standard limit book rule

            expected_trades.append((trade_price, qty))

            order.quantity -= qty
            best.quantity -= qty

            if best.quantity == 0:
                best.active = False
                book_opp.pop(0)
            # price/time unchanged, so no re-sort needed here

        # if still active with remaining qty, add to own book
        if order.quantity > 0 and order.active:
            book_own.append(order)
            sort_books()

    total = len(events)
    for idx, ev in enumerate(events, start=1):
        etype = ev["type"]
        seq += 1

        if idx % 5000 == 0 or idx == total:
            print(f"  processed {idx}/{total} events...")

        if etype == "NEW":
            side = ev["side"]
            price = ev["price"]
            quantity = ev["quantity"]
            user_id = ev.get("user_id")

            if side not in ("buy", "sell"):
                continue
            if price is None or quantity is None or quantity <= 0:
                continue

            # ----- self-trade protection (like 412 in your exchange) -----
            book_opp = asks if side == "buy" else bids
            self_blocked = False
            for resting in book_opp:
                if not resting.active or resting.quantity <= 0:
                    continue
                crosses = (
                    (side == "buy" and price >= resting.price) or
                    (side == "sell" and price <= resting.price)
                )
                if crosses and user_id is not None and resting.user_id == user_id:
                    # order would match own resting order -> reject it
                    self_blocked = True
                    break
            if self_blocked:
                continue  # ignore this NEW event entirely

            o = Order(
                order_id=ev["order_id"],
                side=side,
                price=int(price),
                quantity=int(quantity),
                timestamp=seq,
                user_id=user_id or "",
            )
            orders_by_id[o.order_id] = o
            match_new_order(o)

        elif etype == "MODIFY":
            oid = ev["order_id"]
            o = orders_by_id.get(oid)
            if o is None or not o.active:
                continue

            new_price = ev["price"]
            new_quantity = ev["quantity"]
            if new_price is None or new_quantity is None or new_quantity <= 0:
                # unsupported modify
                continue

            side = o.side
            user_id = o.user_id
            new_price = int(new_price)
            new_quantity = int(new_quantity)

            # ----- self-trade protection for modify -----
            book_opp = asks if side == "buy" else bids
            self_blocked = False
            for resting in book_opp:
                if not resting.active or resting.quantity <= 0:
                    continue
                if resting.order_id == oid:
                    continue
                crosses = (
                    (side == "buy" and new_price >= resting.price) or
                    (side == "sell" and new_price <= resting.price)
                )
                if crosses and user_id and resting.user_id == user_id:
                    # modification would create self-cross -> reject modify
                    self_blocked = True
                    break
            if self_blocked:
                continue  # keep original order unchanged

            # remove from its book
            book = bids if side == "buy" else asks
            if o in book:
                book.remove(o)

            # apply changes
            o.price = new_price
            o.quantity = new_quantity
            o.timestamp = seq  # new time priority
            o.active = True

            sort_books()
            match_new_order(o)

        elif etype == "CANCEL":
            oid = ev["order_id"]
            o = orders_by_id.get(oid)
            if o is None or not o.active:
                continue
            o.active = False
            book = bids if o.side == "buy" else asks
            if o in book:
                book.remove(o)

        else:
            raise ValueError(f"Unknown event type: {etype}")

    return expected_trades


# -------------------- Comparison -------------------- #

def compare_trades(expected: List[Tuple[int, int]], actual: List[Dict]) -> List[str]:
    """
    expected: list of (price, qty)
    actual: list of {"id": ..., "price": ..., "quantity": ...}
    """
    used = [False] * len(actual)
    result_lines: List[str] = []

    # match each expected trade to one actual trade with same price & quantity
    for (ep, eq) in expected:
        found = False
        for i, atr in enumerate(actual):
            if used[i]:
                continue
            if atr["price"] == ep and atr["quantity"] == eq:
                used[i] = True
                found = True
                break
        if not found:
            # missing trade
            result_lines.append(f"-{eq} @ {ep}")

    # any actual trade not matched is extra
    for i, atr in enumerate(actual):
        if not used[i]:
            result_lines.append(f"+{atr['id']}")

    return result_lines


# -------------------- Main -------------------- #

def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} events.sqml trades.sqml output.txt", file=sys.stderr)
        sys.exit(1)

    events_path, trades_path, out_path = sys.argv[1], sys.argv[2], sys.argv[3]

    print("Reading and parsing events...")
    events = parse_events(events_path)

    print("Reading and parsing trades...")
    actual = parse_trades(trades_path)

    print("Simulating expected trades from events...")
    expected = simulate(events)
    print(f"Simulated {len(expected)} expected trades.")

    print("Comparing expected vs actual trades...")
    lines = compare_trades(expected, actual)
    print(f"Found {len(lines)} inconsistencies; writing to '{out_path}'.")

    # \n line endings only; last \n optional – we'll include it
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        for i, line in enumerate(lines):
            if i + 1 < len(lines):
                f.write(line + "\n")
            else:
                f.write(line)

    print("Done.")


if __name__ == "__main__":
    main()
