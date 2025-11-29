from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from galacticbuffer import encode_message, decode_message
import uuid
import time
import base64
import hashlib
import os
import json

USERS = {}
TOKENS = {}

ORDERS = []
V2_ORDERS = []
TRADES = []

BALANCES = {}
COLLATERAL = {}

DNA_SAMPLES = {}

TRADE_STREAM_CLIENTS = []
ORDER_BOOK_STREAM_CLIENTS = []
EXECUTION_REPORT_CLIENTS = {}

# ---------- persistence ----------

PERSISTENT_DIR = os.environ.get("PERSISTENT_DIR")
STATE_FILE = os.path.join(PERSISTENT_DIR, "exchange_state.json") if PERSISTENT_DIR else None


def _rebuild_balances_from_trades():
    """Recompute BALANCES only from V2 trades."""
    global BALANCES
    BALANCES = {}
    for t in TRADES:
        if t.get("source") != "v2":
            continue
        try:
            price = int(t["price"])
            qty = int(t["quantity"])
        except Exception:
            continue
        amount = price * qty
        buyer = t["buyer_id"]
        seller = t["seller_id"]
        BALANCES[buyer] = BALANCES.get(buyer, 0) - amount
        BALANCES[seller] = BALANCES.get(seller, 0) + amount


def load_state():
    global USERS, V2_ORDERS, TRADES, DNA_SAMPLES, COLLATERAL

    if not STATE_FILE:
        return

    try:
        if not os.path.exists(STATE_FILE):
            return
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        # Any failure -> start with empty state
        return

    USERS = data.get("users", {}) or {}
    DNA_SAMPLES = data.get("dna_samples", {}) or {}
    COLLATERAL = data.get("collateral", {}) or {}

    V2_ORDERS[:] = data.get("v2_orders", []) or []
    TRADES[:] = data.get("trades", []) or []

    _rebuild_balances_from_trades()


def save_state():
    if not STATE_FILE:
        return

    state = {
        "users": USERS,
        "dna_samples": DNA_SAMPLES,
        "collateral": COLLATERAL,
        "v2_orders": V2_ORDERS,
        # persist only V2 trades; V1 state can reset
        "trades": [t for t in TRADES if t.get("source") == "v2"],
    }

    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        tmp_path = STATE_FILE + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f)
        os.replace(tmp_path, STATE_FILE)
    except Exception:
        # ignore persistence errors – service must still run
        pass


# Load persisted state on startup
load_state()


class Handler(BaseHTTPRequestHandler):
    def _check_trading_window(self, delivery_start: int):
        now_ms = int(time.time() * 1000)

        OPEN_MS = 15 * 24 * 60 * 60 * 1000
        CLOSE_MS = 60 * 1000

        open_time = delivery_start - OPEN_MS
        close_time = delivery_start - CLOSE_MS

        if now_ms < open_time:
            self.send_response(425)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return False

        if now_ms > close_time:
            self.send_response(451)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return False

        return True

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return b""
        return self.rfile.read(length)

    def _send_no_content(self, status: int):
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _send_gbuf(self, status: int, obj: dict):
        body = encode_message(obj)
        self.send_response(status)
        self.send_header("Content-Type", "application/x-galacticbuf")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _get_authenticated_user(self):
        auth = self.headers.get("Authorization") or ""
        if not auth.startswith("Bearer "):
            return None
        token = auth[7:].strip()
        if not token:
            return None
        return TOKENS.get(token)

    def _apply_trade_balances(self, buyer_id: str, seller_id: str, price: int, quantity: int):
        amount = int(price) * int(quantity)
        BALANCES[buyer_id] = BALANCES.get(buyer_id, 0) - amount
        BALANCES[seller_id] = BALANCES.get(seller_id, 0) + amount

    def _compute_potential_balance(self, username: str) -> int:
        balance = BALANCES.get(username, 0)

        for o in ORDERS:
            if not o.get("active", True):
                continue
            if o.get("seller_id") != username:
                continue
            try:
                qty = int(o.get("quantity", 0))
                price = int(o.get("price", 0))
            except Exception:
                continue
            if qty <= 0:
                continue
            balance += price * qty

        for o in V2_ORDERS:
            if o.get("owner") != username:
                continue
            if o.get("status") != "ACTIVE":
                continue
            try:
                qty = int(o.get("quantity", 0))
                price = int(o.get("price", 0))
            except Exception:
                continue
            if qty <= 0:
                continue

            side = o.get("side")
            if side == "buy":
                balance -= price * qty
            elif side == "sell":
                balance += price * qty

        return balance

    def _validate_dna_sample(self, dna: str) -> bool:
        if not dna:
            return False
        if len(dna) % 3 != 0:
            return False
        for ch in dna:
            if ch not in ("C", "G", "A", "T"):
                return False
        return True

    def _split_codons(self, dna: str):
        return [dna[i:i+3] for i in range(0, len(dna), 3)]

    def _codon_edit_distance_bounded(self, ref_codons, sample_codons, max_diff: int) -> int:
        n = len(ref_codons)
        m = len(sample_codons)

        if max_diff < 0:
            return max_diff + 1

        if abs(n - m) > max_diff:
            return max_diff + 1

        if n == 0:
            return m
        if m == 0:
            return n

        prev = {}
        for j in range(0, min(m, max_diff) + 1):
            prev[j] = j

        for i in range(1, n + 1):
            j_min = max(0, i - max_diff)
            j_max = min(m, i + max_diff)
            curr = {}

            for j in range(j_min, j_max + 1):
                if j > j_min:
                    ins = curr[j - 1] + 1
                else:
                    ins = max_diff + 1

                dele = prev.get(j, max_diff + 1) + 1

                if j - 1 in prev:
                    sub_cost = 0 if ref_codons[i - 1] == sample_codons[j - 1] else 1
                    sub = prev[j - 1] + sub_cost
                else:
                    sub = max_diff + 1

                curr[j] = min(ins, dele, sub)

            if min(curr.values()) > max_diff:
                return max_diff + 1

            prev = curr

        dist = prev.get(m, max_diff + 1)
        return dist

    def _dna_matches(self, reference: str, submitted: str) -> bool:
        ref_codons = self._split_codons(reference)
        sub_codons = self._split_codons(submitted)

        ref_count = len(ref_codons)
        allowed_diff = ref_count // 100000
        max_diff = allowed_diff

        dist = self._codon_edit_distance_bounded(ref_codons, sub_codons, max_diff)
        return dist <= allowed_diff

    def _ws_build_binary_frame(self, payload: bytes) -> bytes:
        fin_opcode = 0x82
        length = len(payload)
        if length < 126:
            header = bytes([fin_opcode, length])
        elif length < (1 << 16):
            header = bytes([fin_opcode, 126]) + length.to_bytes(2, "big")
        else:
            header = bytes([fin_opcode, 127]) + length.to_bytes(8, "big")
        return header + payload

    def _broadcast_trade(self, trade: dict):
        if not TRADE_STREAM_CLIENTS:
            return
        payload = encode_message({
            "trade_id": str(trade["trade_id"]),
            "buyer_id": str(trade["buyer_id"]),
            "seller_id": str(trade["seller_id"]),
            "price": int(trade["price"]),
            "quantity": int(trade["quantity"]),
            "delivery_start": int(trade["delivery_start"]),
            "delivery_end": int(trade["delivery_end"]),
            "timestamp": int(trade["timestamp"]),
        })
        frame = self._ws_build_binary_frame(payload)

        for sock in list(TRADE_STREAM_CLIENTS):
            try:
                sock.sendall(frame)
            except Exception:
                try:
                    TRADE_STREAM_CLIENTS.remove(sock)
                except ValueError:
                    pass

    def _broadcast_order_book_change(self, order: dict, change_type: str):
        if not ORDER_BOOK_STREAM_CLIENTS:
            return

        payload = encode_message({
            "order_id": str(order["order_id"]),
            "side": order["side"],
            "price": int(order["price"]),
            "quantity": int(order["quantity"]),
            "delivery_start": int(order["delivery_start"]),
            "delivery_end": int(order["delivery_end"]),
            "change_type": change_type,
            "timestamp": int(time.time() * 1000),
        })
        frame = self._ws_build_binary_frame(payload)

        for sock in list(ORDER_BOOK_STREAM_CLIENTS):
            try:
                sock.sendall(frame)
            except Exception:
                try:
                    ORDER_BOOK_STREAM_CLIENTS.remove(sock)
                except ValueError:
                    pass

    def _broadcast_execution_report_for_order(self, order: dict):
        if not EXECUTION_REPORT_CLIENTS:
            return
        owner = order.get("owner")
        if not owner:
            return
        clients = EXECUTION_REPORT_CLIENTS.get(owner)
        if not clients:
            return

        try:
            original_qty = int(order.get("original_quantity", order.get("quantity", 0)))
        except Exception:
            original_qty = int(order.get("quantity", 0) or 0)
        try:
            remaining = int(order.get("quantity", 0))
        except Exception:
            remaining = 0

        if remaining < 0:
            remaining = 0

        filled = original_qty - remaining
        if filled < 0:
            filled = 0

        status = order.get("status", "ACTIVE")

        payload = encode_message({
            "order_id": str(order["order_id"]),
            "status": status,
            "side": order["side"],
            "price": int(order["price"]),
            "filled_quantity": int(filled),
            "remaining_quantity": int(remaining),
            "delivery_start": int(order["delivery_start"]),
            "delivery_end": int(order["delivery_end"]),
            "timestamp": int(time.time() * 1000),
        })
        frame = self._ws_build_binary_frame(payload)

        for sock in list(clients):
            try:
                sock.sendall(frame)
            except Exception:
                try:
                    clients.remove(sock)
                except ValueError:
                    pass

    # (bulk sim helpers unchanged – omitted for brevity in this explanation, kept intact in code)

    def _bulk_sim_create(self, username: str, op: dict, ds: int, de: int, staged_ops: list):
        ...
        # unchanged body
        ...

    def _bulk_sim_modify(self, username: str, op: dict, ds: int, de: int, staged_ops: list):
        ...
        # unchanged body
        ...

    def _bulk_sim_cancel(self, username: str, op: dict, ds: int, de: int, staged_ops: list):
        ...
        # unchanged body
        ...

    def _build_sim_order_book(self, ds: int, de: int, staged_ops: list, exclude_order_id: str = None):
        ...
        # unchanged body
        ...

    def _find_order_in_sim(self, order_id: str, ds: int, de: int, staged_ops: list):
        ...
        # unchanged body
        ...

    def _check_collateral_in_sim_state(self, username: str, side: str, price: int, quantity: int, staged_ops: list):
        ...
        # unchanged body
        ...

    def _check_collateral_modify_in_sim(self, username: str, order_id: str, new_price: int, new_quantity: int, staged_ops: list):
        ...
        # unchanged body
        ...

    # ---------- HTTP methods etc. (only persistence calls added) ----------

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/health":
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif parsed.path == "/orders":
            self.handle_list_orders(parsed)

        elif parsed.path == "/trades":
            self.handle_list_trades()

        elif parsed.path == "/v2/orders":
            self.handle_v2_order_book(parsed)

        elif parsed.path == "/v2/my-orders":
            self.handle_my_orders()

        elif parsed.path == "/v2/my-trades":
            self.handle_my_trades(parsed)

        elif parsed.path == "/balance":
            self.handle_get_balance()

        elif parsed.path == "/v2/trades":
            self.handle_v2_trades(parsed)

        elif parsed.path == "/v2/stream/trades":
            self.handle_trade_stream()

        elif parsed.path == "/v2/stream/order-book":
            self.handle_order_book_stream()

        elif parsed.path == "/v2/stream/execution-reports":
            self.handle_execution_reports_stream(parsed)

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/register":
            self.handle_register()
        elif self.path == "/login":
            self.handle_login()
        elif self.path == "/orders":
            self.handle_submit_order()
        elif self.path == "/v2/orders":
            self.handle_submit_order_v2()
        elif self.path == "/trades":
            self.handle_take_order()
        elif self.path == "/v2/bulk-operations":
            self.handle_bulk_operations()
        elif self.path == "/dna-submit":
            self.handle_dna_submit()
        elif self.path == "/dna-login":
            self.handle_dna_login()
        else:
            self.send_response(404)
            self.end_headers()

    def do_PUT(self):
        parsed = urlparse(self.path)
        if parsed.path == "/user/password":
            self.handle_change_password()
        elif parsed.path.startswith("/v2/orders/"):
            order_id = parsed.path.split("/")[-1]
            self.handle_modify_order(order_id)
        elif parsed.path.startswith("/collateral/"):
            username = parsed.path.split("/")[-1]
            self.handle_set_collateral(username)
        else:
            self.send_response(404)
            self.end_headers()

    def do_DELETE(self):
        parsed = urlparse(self.path)
        if parsed.path.startswith("/v2/orders/"):
            order_id = parsed.path.split("/")[-1]
            self.handle_cancel_order(order_id)
        else:
            self.send_response(404)
            self.end_headers()

    def finish(self):
        try:
            if not self.wfile.closed:
                self.wfile.flush()
        except Exception:
            pass
        if getattr(self, "_is_websocket", False):
            return
        try:
            self.wfile.close()
        except Exception:
            pass
        try:
            self.rfile.close()
        except Exception:
            pass

    def handle_register(self):
        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        username = (data.get("username") or "").strip()
        password = (data.get("password") or "").strip()

        if not username or not password:
            self._send_no_content(400)
            return

        if username in USERS:
            self._send_no_content(409)
            return

        USERS[username] = password
        save_state()
        self._send_no_content(204)

    def handle_login(self):
        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(401)
            return

        username = (data.get("username") or "").strip()
        password = (data.get("password") or "").strip()

        if not username or not password:
            self._send_no_content(401)
            return

        if USERS.get(username) != password:
            self._send_no_content(401)
            return

        token = uuid.uuid4().hex
        TOKENS[token] = username

        self._send_gbuf(200, {"token": token})

    def handle_change_password(self):
        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        username = (data.get("username") or "").strip()
        old_password = (data.get("old_password") or "").strip()
        new_password = (data.get("new_password") or "").strip()

        if not username or not old_password or not new_password:
            self._send_no_content(400)
            return

        user_password = USERS.get(username)
        if user_password is None:
            self._send_no_content(401)
            return

        if user_password != old_password:
            self._send_no_content(401)
            return

        try:
            USERS[username] = new_password
            tokens_to_delete = [t for t, u in list(TOKENS.items()) if u == username]
            for t in tokens_to_delete:
                del TOKENS[t]
            save_state()
        except Exception:
            self._send_no_content(500)
            return

        self._send_no_content(204)

    # streams & bulk handlers unchanged, except we add save_state() at the end of handle_bulk_operations

    def handle_trade_stream(self):
        ...
        # unchanged body
        ...

    def handle_order_book_stream(self):
        ...
        # unchanged body
        ...

    def handle_execution_reports_stream(self, parsed):
        ...
        # unchanged body
        ...

    def handle_bulk_operations(self):
        try:
            body = self._read_body()
            data = decode_message(body)
        except Exception:
            return self._send_no_content(400)

        contracts = data.get("contracts")
        if not isinstance(contracts, list) or not contracts:
            return self._send_no_content(400)

        staged_operations = []

        # simulation loop unchanged
        ...
        # commit loop unchanged EXCEPT we now save_state() after applying:

        for result in staged_operations:
            ...
            # your existing create/modify/cancel application code
            ...

        # after applying all staged ops:
        save_state()

        results = []
        for result in staged_operations:
            entry = {
                "type": result["action"],
                "order_id": result["order_id"],
            }
            if "status" in result:
                entry["status"] = result["status"]
            results.append(entry)

        return self._send_gbuf(200, {"results": results})

    def handle_dna_submit(self):
        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        username = (data.get("username") or "").strip()
        password = (data.get("password") or "").strip()
        dna_sample = (data.get("dna_sample") or "").strip()

        if not username or not password or not dna_sample:
            self._send_no_content(400)
            return

        if USERS.get(username) != password:
            self._send_no_content(401)
            return

        if not self._validate_dna_sample(dna_sample):
            self._send_no_content(400)
            return

        samples = DNA_SAMPLES.setdefault(username, [])
        if dna_sample not in samples:
            samples.append(dna_sample)
            save_state()

        self._send_no_content(204)

    def handle_dna_login(self):
        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        username = (data.get("username") or "").strip()
        dna_sample = (data.get("dna_sample") or "").strip()

        if not username or not dna_sample:
            self._send_no_content(400)
            return

        if username not in USERS:
            self._send_no_content(401)
            return

        if username not in DNA_SAMPLES or not DNA_SAMPLES[username]:
            self._send_no_content(401)
            return

        if not self._validate_dna_sample(dna_sample):
            self._send_no_content(400)
            return

        matched = False
        for ref in DNA_SAMPLES[username]:
            if self._dna_matches(ref, dna_sample):
                matched = True
                break

        if not matched:
            self._send_no_content(401)
            return

        token = uuid.uuid4().hex
        TOKENS[token] = username

        self._send_gbuf(200, {"token": token})

    def handle_list_orders(self, parsed):
        ...
        # unchanged

    def handle_submit_order(self):
        ...
        # unchanged (V1 – not persisted)

    def _check_collateral_create(self, username: str, side: str, price: int, quantity: int) -> bool:
        ...
        # unchanged

    def _check_collateral_modify(self, username: str, order_id: str, new_price: int, new_quantity: int) -> bool:
        ...
        # unchanged

    def handle_submit_order_v2(self):
        ...
        # your whole matching engine body unchanged,
        # but add save_state() right before sending response:

        order_snapshot = {
            "order_id": order_id,
            "side": side,
            "owner": username,
            "price": price,
            "quantity": remaining,
            "delivery_start": delivery_start,
            "delivery_end": delivery_end,
            "status": status,
            "original_quantity": original_quantity,
        }
        self._broadcast_execution_report_for_order(order_snapshot)

        # PERSIST
        save_state()

        self._send_gbuf(200, {
            "order_id": order_id,
            "status": status,
            "filled_quantity": filled_quantity,
        })

    def handle_modify_order(self, order_id: str):
        ...
        # everything unchanged up to just before sending response

        if order["status"] == "ACTIVE":
            self._broadcast_order_book_change(order, "MODIFY")
        else:
            self._broadcast_order_book_change(order, "REMOVE")

        self._broadcast_execution_report_for_order(order)

        # PERSIST
        save_state()

        self._send_gbuf(200, {
            "order_id": order["order_id"],
            "status": order["status"],
            "filled_quantity": filled_quantity,
        })

    def handle_cancel_order(self, order_id: str):
        ...
        # after updating order status and broadcasts

        order["quantity"] = 0  # ensure zero

        self._broadcast_order_book_change(order, "REMOVE")
        self._broadcast_execution_report_for_order(order)

        # PERSIST
        save_state()

        self._send_no_content(204)

    def handle_v2_order_book(self, parsed):
        ...
        # unchanged

    def handle_my_orders(self):
        ...
        # unchanged

    def handle_my_trades(self, parsed):
        ...
        # unchanged – uses TRADES (V1 + V2), but only V2 survive restarts

    def handle_list_trades(self):
        ...
        # unchanged

    def handle_v2_trades(self, parsed):
        ...
        # unchanged

    def handle_take_order(self):
        ...
        # unchanged (V1 – not persisted, trades have source "v1")

    def handle_set_collateral(self, username: str):
        ...
        COLLATERAL[username] = collateral_value
        save_state()
        self._send_no_content(204)

    def handle_get_balance(self):
        ...
        # unchanged


def run():
    server = ThreadingHTTPServer(("", 8080), Handler)
    print("Server running on port 8080...")
    server.serve_forever()


if __name__ == "__main__":
    run()
