from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from galacticbuffer import encode_message, decode_message
import uuid
import time
import base64
import hashlib

USERS = {}
TOKENS = {}

ORDERS = []
V2_ORDERS = []
TRADES = []

# New: balances + collateral
BALANCES = {}     # username -> int
COLLATERAL = {}   # username -> collateral limit (None = unlimited)

# New: DNA samples
# username -> list of registered DNA strings
DNA_SAMPLES = {}

# WebSocket trade stream clients (raw sockets)
TRADE_STREAM_CLIENTS = []


class Handler(BaseHTTPRequestHandler):
    # ---------- helpers ----------

    def _check_trading_window(self, delivery_start: int):
        """
        Returns:
            True  - inside trading window
            False - already responded with 425/451
        """

        now_ms = int(time.time() * 1000)

        OPEN_MS = 15 * 24 * 60 * 60 * 1000     # 15 days
        CLOSE_MS = 60 * 1000                   # 1 min

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

    # ----- balances / collateral helpers -----

    def _apply_trade_balances(self, buyer_id: str, seller_id: str, price: int, quantity: int):
        """
        Buyer pays price * quantity, seller receives price * quantity.
        Negative prices will naturally invert the effect.
        """
        amount = int(price) * int(quantity)
        BALANCES[buyer_id] = BALANCES.get(buyer_id, 0) - amount
        BALANCES[seller_id] = BALANCES.get(seller_id, 0) + amount

    def _compute_potential_balance(self, username: str) -> int:
        """
        Potential balance = current balance + effect if all ACTIVE orders fill.

        Includes:
          - V1 ORDERS (always sells at positive price)
          - V2_ORDERS (buys/sells with side)
        """
        balance = BALANCES.get(username, 0)

        # --- V1 orders: always sells made by seller_id ---
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
            # V1 is always sell at positive price -> user receives price * qty
            balance += price * qty

        # --- V2 orders: buys and sells ---
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
                # Buy: user pays price * qty
                balance -= price * qty
            elif side == "sell":
                # Sell: user receives price * qty
                balance += price * qty

        return balance

    # ---------- DNA helpers ----------

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
        """
        Levenshtein distance on codons with a hard cap max_diff.
        Returns a value > max_diff if distance exceeds max_diff.
        Uses a banded DP of width ~2*max_diff+1.
        """
        n = len(ref_codons)
        m = len(sample_codons)

        if max_diff < 0:
            return max_diff + 1

        # At least this many insert/delete ops
        if abs(n - m) > max_diff:
            return max_diff + 1

        if n == 0:
            return m
        if m == 0:
            return n

        # prev and curr are dicts: j -> cost
        prev = {}
        # row 0: cost = j (0..min(m, max_diff))
        for j in range(0, min(m, max_diff) + 1):
            prev[j] = j

        for i in range(1, n + 1):
            j_min = max(0, i - max_diff)
            j_max = min(m, i + max_diff)
            curr = {}

            for j in range(j_min, j_max + 1):
                # insertion: (i, j-1) -> (i, j)
                if j > j_min:
                    ins = curr[j - 1] + 1
                else:
                    ins = max_diff + 1  # out of band

                # deletion: (i-1, j) -> (i, j)
                dele = prev.get(j, max_diff + 1) + 1

                # substitution / match: (i-1, j-1) -> (i, j)
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
        allowed_diff = ref_count // 100000  # floor(Ca/100000)

        # If ref is very short, allowed_diff might be 0 -> exact or within 0 edits
        max_diff = allowed_diff

        dist = self._codon_edit_distance_bounded(ref_codons, sub_codons, max_diff)
        return dist <= allowed_diff

    # ---------- WebSocket helpers ----------

    def _ws_build_binary_frame(self, payload: bytes) -> bytes:
        # Server-to-client frames are not masked.
        fin_opcode = 0x82  # FIN=1, opcode=2 (binary)
        length = len(payload)
        if length < 126:
            header = bytes([fin_opcode, length])
        elif length < (1 << 16):
            header = bytes([fin_opcode, 126]) + length.to_bytes(2, "big")
        else:
            header = bytes([fin_opcode, 127]) + length.to_bytes(8, "big")
        return header + payload

    def _broadcast_trade(self, trade: dict):
        # Only V2 trades should be broadcast; caller ensures this
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

        # Send to all connected clients, drop broken ones
        for sock in list(TRADE_STREAM_CLIENTS):
            try:
                sock.sendall(frame)
            except Exception:
                try:
                    TRADE_STREAM_CLIENTS.remove(sock)
                except ValueError:
                    pass

    # ---------- HTTP methods ----------

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
            # NEW: public V2 trades endpoint
            self.handle_v2_trades(parsed)

        elif parsed.path == "/v2/stream/trades":
            self.handle_trades_stream()

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

    # Override finish so WebSocket connections stay open
    def finish(self):
        try:
            if not self.wfile.closed:
                self.wfile.flush()
        except Exception:
            pass
        if getattr(self, "_is_websocket", False):
            # Do not close socket for WebSocket connections
            return
        try:
            self.wfile.close()
        except Exception:
            pass
        try:
            self.rfile.close()
        except Exception:
            pass

    # ---------- auth & users ----------

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
        except Exception:
            self._send_no_content(500)
            return

        self._send_no_content(204)

    # ---------- DNA endpoints ----------

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

        # Validate input presence
        if not username or not password or not dna_sample:
            self._send_no_content(400)
            return

        # Check credentials (username/password)
        if USERS.get(username) != password:
            self._send_no_content(401)
            return

        # Validate DNA format
        if not self._validate_dna_sample(dna_sample):
            self._send_no_content(400)
            return

        # Register DNA; duplicate samples are fine (idempotent)
        samples = DNA_SAMPLES.setdefault(username, [])
        if dna_sample not in samples:
            samples.append(dna_sample)

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

        # Validate input presence
        if not username or not dna_sample:
            self._send_no_content(400)
            return

        # User must exist and have DNA registered
        if username not in USERS:
            self._send_no_content(401)
            return

        if username not in DNA_SAMPLES or not DNA_SAMPLES[username]:
            self._send_no_content(401)
            return

        # Validate DNA format
        if not self._validate_dna_sample(dna_sample):
            self._send_no_content(400)
            return

        # Compare against all reference samples
        matched = False
        for ref in DNA_SAMPLES[username]:
            if self._dna_matches(ref, dna_sample):
                matched = True
                break

        if not matched:
            self._send_no_content(401)
            return

        # Success → issue token just like /login
        token = uuid.uuid4().hex
        TOKENS[token] = username

        self._send_gbuf(200, {"token": token})

    # ---------- V1 orders & trades ----------

    def handle_list_orders(self, parsed):
        qs = parse_qs(parsed.query)

        if "delivery_start" not in qs or "delivery_end" not in qs:
            self._send_no_content(400)
            return

        try:
            delivery_start = int(qs["delivery_start"][0])
            delivery_end = int(qs["delivery_end"][0])
        except Exception:
            self._send_no_content(400)
            return

        matching = [
            o for o in ORDERS
            if o.get("active", True)
            and int(o.get("delivery_start", 0)) == delivery_start
            and int(o.get("delivery_end", 0)) == delivery_end
        ]

        matching.sort(key=lambda o: int(o["price"]))

        orders_payload = []
        for o in matching:
            orders_payload.append({
                "order_id": str(o["order_id"]),
                "price": int(o["price"]),
                "quantity": int(o["quantity"]),
                "delivery_start": int(o["delivery_start"]),
                "delivery_end": int(o["delivery_end"]),
            })

        self._send_gbuf(200, {"orders": orders_payload})

    def handle_submit_order(self):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        try:
            price = int(data.get("price"))
            quantity = int(data.get("quantity"))
            delivery_start = int(data.get("delivery_start"))
            delivery_end = int(data.get("delivery_end"))
        except Exception:
            self._send_no_content(400)
            return

        if quantity <= 0:
            self._send_no_content(400)
            return

        HOUR_MS = 3600000
        if (delivery_start % HOUR_MS) != 0 or (delivery_end % HOUR_MS) != 0:
            self._send_no_content(400)
            return
        if delivery_end - delivery_start != HOUR_MS:
            self._send_no_content(400)
            return

        order_id = uuid.uuid4().hex
        order = {
            "order_id": order_id,
            "seller_id": username,
            "price": price,
            "quantity": quantity,
            "delivery_start": delivery_start,
            "delivery_end": delivery_end,
            "active": True,
        }
        ORDERS.append(order)

        self._send_gbuf(200, {"order_id": order_id})

    # ---------- collateral checks ----------

    def _check_collateral_create(self, username: str, side: str, price: int, quantity: int) -> bool:
        coll = COLLATERAL.get(username)
        if coll is None:
            return True  # unlimited
        # Only check orders that can reduce balance
        if not ((side == "buy" and price > 0) or (side == "sell" and price < 0)):
            return True
        base = self._compute_potential_balance(username)
        if side == "buy":
            delta = -price * quantity
        else:
            delta = price * quantity
        potential_after = base + delta
        return potential_after >= -coll

    def _check_collateral_modify(self, username: str, order_id: str, new_price: int, new_quantity: int) -> bool:
        coll = COLLATERAL.get(username)
        if coll is None:
            return True

        # Recompute potential balance assuming the target order has (new_price, new_quantity)
        base = BALANCES.get(username, 0)
        side_for_target = None

        for o in V2_ORDERS:
            if o.get("owner") != username:
                continue
            if o.get("status") != "ACTIVE":
                continue
            qty = int(o.get("quantity", 0))
            if qty <= 0:
                continue
            price = int(o["price"])
            side = o["side"]

            if o.get("order_id") == order_id:
                qty = new_quantity
                price = new_price
                side = o["side"]
                side_for_target = side

            if side == "buy":
                base -= price * qty
            else:
                base += price * qty

        if side_for_target is None:
            # Order not found as active – let other logic handle 404
            return True

        if not ((side_for_target == "buy" and new_price > 0) or (side_for_target == "sell" and new_price < 0)):
            return True

        return base >= -coll

    # ---------- V2 core create/modify/cancel helpers ----------

    def _v2_create_core(self, username: str, side: str, price: int, quantity: int,
                        delivery_start: int, delivery_end: int, execution_type: str):
        if execution_type not in ("GTC", "IOC", "FOK"):
            self._send_no_content(400)
            return None

        if side not in ("buy", "sell"):
            self._send_no_content(400)
            return None

        if quantity <= 0:
            self._send_no_content(400)
            return None

        HOUR_MS = 3600000
        if (delivery_start % HOUR_MS) != 0 or (delivery_end % HOUR_MS) != 0:
            self._send_no_content(400)
            return None
        if delivery_end <= delivery_start:
            self._send_no_content(400)
            return None
        if delivery_end - delivery_start != HOUR_MS:
            self._send_no_content(400)
            return None

        if not self._check_trading_window(delivery_start):
            return None

        # Collateral check BEFORE matching/adding order
        if not self._check_collateral_create(username, side, price, quantity):
            self.send_response(402)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return None

        order_id = uuid.uuid4().hex
        now_ms = int(time.time() * 1000)

        remaining = quantity
        filled_quantity = 0

        # Build candidate list on opposite side
        if side == "buy":
            candidates = [
                o for o in V2_ORDERS
                if o.get("status") == "ACTIVE"
                and o["side"] == "sell"
                and o["delivery_start"] == delivery_start
                and o["delivery_end"] == delivery_end
                and o["quantity"] > 0
                and o["price"] <= price
            ]
            # Cheapest sells first, then oldest
            candidates.sort(key=lambda o: (o["price"], o.get("created_at", 0)))
        else:  # sell
            candidates = [
                o for o in V2_ORDERS
                if o.get("status") == "ACTIVE"
                and o["side"] == "buy"
                and o["delivery_start"] == delivery_start
                and o["delivery_end"] == delivery_end
                and o["quantity"] > 0
                and o["price"] >= price
            ]
            # Highest bids first, then oldest
            candidates.sort(key=lambda o: (-o["price"], o.get("created_at", 0)))

        # Self-match prevention BEFORE any trades / book changes
        for resting in candidates:
            if resting.get("owner") == username:
                self._send_no_content(412)
                return None

        # FOK: dry-run to see if full quantity can be filled immediately
        if execution_type == "FOK":
            total_possible = 0
            for resting in candidates:
                if resting.get("status") != "ACTIVE" or resting["quantity"] <= 0:
                    continue
                total_possible += resting["quantity"]
                if total_possible >= quantity:
                    break

            if total_possible < quantity:
                # Cannot fully fill -> cancel, no trades, no book entry
                return {
                    "order_id": order_id,
                    "status": "CANCELLED",
                    "filled_quantity": 0,
                }

        # Matching loop (used by all types once we've passed FOK dry-run)
        for resting in candidates:
            if remaining <= 0:
                break

            if resting.get("status") != "ACTIVE" or resting["quantity"] <= 0:
                continue

            trade_qty = min(remaining, resting["quantity"])
            if trade_qty <= 0:
                continue

            if side == "buy":
                buyer_id = username
                seller_id = resting["owner"]
            else:
                buyer_id = resting["owner"]
                seller_id = username

            trade_price = resting["price"]  # maker price
            trade_id = uuid.uuid4().hex
            ts = int(time.time() * 1000)

            trade = {
                "trade_id": trade_id,
                "buyer_id": buyer_id,
                "seller_id": seller_id,
                "price": trade_price,
                "quantity": trade_qty,
                "timestamp": ts,
                "delivery_start": delivery_start,
                "delivery_end": delivery_end,
                "source": "v2",  # mark as V2 trade
            }
            TRADES.append(trade)
            self._apply_trade_balances(buyer_id, seller_id, trade_price, trade_qty)
            # Broadcast to stream subscribers
            self._broadcast_trade(trade)

            remaining -= trade_qty
            filled_quantity += trade_qty

            resting["quantity"] -= trade_qty
            if resting["quantity"] <= 0:
                resting["quantity"] = 0
                resting["status"] = "FILLED"

        # Decide final status and whether order goes into book
        if execution_type == "GTC":
            if remaining > 0:
                status = "ACTIVE"
                V2_ORDERS.append({
                    "order_id": order_id,
                    "side": side,
                    "owner": username,
                    "price": price,
                    "quantity": remaining,
                    "delivery_start": delivery_start,
                    "delivery_end": delivery_end,
                    "status": "ACTIVE",
                    "created_at": now_ms,
                })
            else:
                status = "FILLED"
        elif execution_type == "IOC":
            # Never go into book; remaining is cancelled
            status = "FILLED" if remaining <= 0 else "CANCELLED"
        else:  # FOK
            # We already ensured via dry-run that we can fully fill
            # so remaining should be 0 here.
            status = "FILLED"

        return {
            "order_id": order_id,
            "status": status,
            "filled_quantity": filled_quantity,
        }

    def _v2_modify_core(self, username: str, order_id: str, new_price: int, new_quantity: int):
        if new_quantity <= 0:
            self._send_no_content(400)
            return None

        order = None
        for o in V2_ORDERS:
            if o.get("order_id") == order_id:
                order = o
                break

        if not order or order.get("status") != "ACTIVE" or order["quantity"] <= 0:
            self._send_no_content(404)
            return None

        if order.get("owner") != username:
            self._send_no_content(403)
            return None

        side = order["side"]
        delivery_start = order["delivery_start"]
        delivery_end = order["delivery_end"]

        # Self-match prevention: compute candidates with new price BEFORE mutating order
        if side == "buy":
            candidates = [
                o for o in V2_ORDERS
                if o.get("status") == "ACTIVE"
                and o["side"] == "sell"
                and o["delivery_start"] == delivery_start
                and o["delivery_end"] == delivery_end
                and o["quantity"] > 0
                and o["order_id"] != order_id
                and o["price"] <= new_price
            ]
            candidates.sort(key=lambda o: (o["price"], o.get("created_at", 0)))
        else:
            candidates = [
                o for o in V2_ORDERS
                if o.get("status") == "ACTIVE"
                and o["side"] == "buy"
                and o["delivery_start"] == delivery_start
                and o["delivery_end"] == delivery_end
                and o["quantity"] > 0
                and o["order_id"] != order_id
                and o["price"] >= new_price
            ]
            candidates.sort(key=lambda o: (-o["price"], o.get("created_at", 0)))

        for resting in candidates:
            if resting.get("owner") == username:
                self._send_no_content(412)
                return None

        # Collateral check with new values
        if not self._check_collateral_modify(username, order_id, new_price, new_quantity):
            self.send_response(402)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return None

        old_price = order["price"]
        old_quantity = order["quantity"]

        # Apply modifications
        order["price"] = new_price
        order["quantity"] = new_quantity

        now_ms = int(time.time() * 1000)
        if new_price != old_price or new_quantity > old_quantity:
            order["created_at"] = now_ms

        remaining = order["quantity"]
        filled_quantity = 0

        # Matching loop using precomputed candidates
        for resting in candidates:
            if remaining <= 0:
                break
            if resting.get("status") != "ACTIVE" or resting["quantity"] <= 0:
                continue

            if side == "buy" and new_price < resting["price"]:
                continue
            if side == "sell" and new_price > resting["price"]:
                continue

            trade_qty = min(remaining, resting["quantity"])
            if trade_qty <= 0:
                continue

            if side == "buy":
                buyer_id = username
                seller_id = resting["owner"]
            else:
                buyer_id = resting["owner"]
                seller_id = username

            trade_price = resting["price"]
            trade_id = uuid.uuid4().hex
            ts = int(time.time() * 1000)

            trade = {
                "trade_id": trade_id,
                "buyer_id": buyer_id,
                "seller_id": seller_id,
                "price": trade_price,
                "quantity": trade_qty,
                "timestamp": ts,
                "delivery_start": delivery_start,
                "delivery_end": delivery_end,
                "source": "v2",  # mark as V2 trade
            }
            TRADES.append(trade)
            self._apply_trade_balances(buyer_id, seller_id, trade_price, trade_qty)
            self._broadcast_trade(trade)

            remaining -= trade_qty
            filled_quantity += trade_qty
            resting["quantity"] -= trade_qty
            if resting["quantity"] <= 0:
                resting["quantity"] = 0
                resting["status"] = "FILLED"

        order["quantity"] = remaining
        if remaining <= 0:
            order["quantity"] = 0
            order["status"] = "FILLED"

        return {
            "order_id": order["order_id"],
            "status": order["status"],
            "filled_quantity": filled_quantity,
        }

    def _v2_cancel_core(self, username: str, order_id: str):
        order = None
        for o in V2_ORDERS:
            if o.get("order_id") == order_id:
                order = o
                break

        if not order or order.get("status") != "ACTIVE" or order["quantity"] <= 0:
            self._send_no_content(404)
            return False

        if order.get("owner") != username:
            self._send_no_content(403)
            return False

        order["status"] = "CANCELLED"
        order["quantity"] = 0
        return True

    # ---------- V2 submit (matching engine + IOC/FOK) ----------

    def handle_submit_order_v2(self):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        side = (data.get("side") or "").strip()
        execution_type = (data.get("execution_type") or "GTC").strip() or "GTC"

        try:
            price = int(data.get("price"))
            quantity = int(data.get("quantity"))
            delivery_start = int(data.get("delivery_start"))
            delivery_end = int(data.get("delivery_end"))
        except Exception:
            self._send_no_content(400)
            return

        res = self._v2_create_core(
            username=username,
            side=side,
            price=price,
            quantity=quantity,
            delivery_start=delivery_start,
            delivery_end=delivery_end,
            execution_type=execution_type,
        )
        if res is None:
            return

        self._send_gbuf(200, res)

    # ---------- V2 modify / cancel ----------

    def handle_modify_order(self, order_id: str):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        if "price" not in data or "quantity" not in data:
            self._send_no_content(400)
            return

        try:
            new_price = int(data.get("price"))
            new_quantity = int(data.get("quantity"))
        except Exception:
            self._send_no_content(400)
            return

        res = self._v2_modify_core(username, order_id, new_price, new_quantity)
        if res is None:
            return

        self._send_gbuf(200, res)

    def handle_cancel_order(self, order_id: str):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        ok = self._v2_cancel_core(username, order_id)
        if not ok:
            return

        self._send_no_content(204)

    # ---------- V2 order book / my-orders / my-trades ----------

    def handle_v2_order_book(self, parsed):
        qs = parse_qs(parsed.query)
        if "delivery_start" not in qs or "delivery_end" not in qs:
            self._send_no_content(400)
            return

        try:
            delivery_start = int(qs["delivery_start"][0])
            delivery_end = int(qs["delivery_end"][0])
        except Exception:
            self._send_no_content(400)
            return

        HOUR_MS = 3600000
        if (delivery_start % HOUR_MS) != 0 or (delivery_end % HOUR_MS) != 0:
            self._send_no_content(400)
            return
        if delivery_end <= delivery_start or delivery_end - delivery_start != HOUR_MS:
            self._send_no_content(400)
            return

        # Trading window: outside window → empty orderbook
        OPEN_MS = 15 * 24 * 60 * 60 * 1000
        CLOSE_MS = 60 * 1000
        now_ms = int(time.time() * 1000)

        open_time = delivery_start - OPEN_MS
        close_time = delivery_start - CLOSE_MS

        if not (open_time <= now_ms <= close_time):
            return self._send_gbuf(200, {"bids": [], "asks": []})

        bids = []
        asks = []

        for o in V2_ORDERS:
            if o.get("status") != "ACTIVE":
                continue
            if o["quantity"] <= 0:
                continue
            if o["delivery_start"] != delivery_start or o["delivery_end"] != delivery_end:
                continue

            entry = {
                "order_id": o["order_id"],
                "price": o["price"],
                "quantity": o["quantity"],
            }

            if o["side"] == "buy":
                bids.append((o, entry))
            else:
                asks.append((o, entry))

        # Bids: highest price first, then oldest
        bids.sort(key=lambda x: (-x[0]["price"], x[0].get("created_at", 0)))
        # Asks: lowest price first, then oldest
        asks.sort(key=lambda x: (x[0]["price"], x[0].get("created_at", 0)))

        bids_payload = [e for _, e in bids]
        asks_payload = [e for _, e in asks]

        self._send_gbuf(200, {"bids": bids_payload, "asks": asks_payload})

    def handle_my_orders(self):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        my_active = [
            o for o in V2_ORDERS
            if o.get("owner") == username
            and o.get("status") == "ACTIVE"
            and o["quantity"] > 0
        ]

        my_active.sort(key=lambda o: o.get("created_at", 0), reverse=True)

        orders_payload = []
        for o in my_active:
            orders_payload.append({
                "order_id": o["order_id"],
                "side": o["side"],
                "price": o["price"],
                "quantity": o["quantity"],
                "delivery_start": o["delivery_start"],
                "delivery_end": o["delivery_end"],
                "timestamp": o.get("created_at", 0),
            })

        self._send_gbuf(200, {"orders": orders_payload})

    def handle_my_trades(self, parsed):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        qs = parse_qs(parsed.query)
        if "delivery_start" not in qs or "delivery_end" not in qs:
            self._send_no_content(400)
            return

        try:
            delivery_start = int(qs["delivery_start"][0])
            delivery_end = int(qs["delivery_end"][0])
        except Exception:
            self._send_no_content(400)
            return

        HOUR_MS = 3600000
        if (delivery_start % HOUR_MS) != 0 or (delivery_end % HOUR_MS) != 0:
            self._send_no_content(400)
            return
        if delivery_end <= delivery_start or delivery_end - delivery_start != HOUR_MS:
            self._send_no_content(400)
            return

        my_trades = []
        for t in TRADES:
            if t.get("delivery_start") != delivery_start or t.get("delivery_end") != delivery_end:
                continue
            buyer = t["buyer_id"]
            seller = t["seller_id"]
            if buyer != username and seller != username:
                continue

            if buyer == username:
                side = "buy"
                counterparty = seller
            else:
                side = "sell"
                counterparty = buyer

            my_trades.append({
                "trade_id": t["trade_id"],
                "side": side,
                "price": int(t["price"]),
                "quantity": int(t["quantity"]),
                "counterparty": counterparty,
                "delivery_start": int(t["delivery_start"]),
                "delivery_end": int(t["delivery_end"]),
                "timestamp": int(t["timestamp"]),
            })

        my_trades.sort(key=lambda tr: tr["timestamp"], reverse=True)

        self._send_gbuf(200, {"trades": my_trades})

    # ---------- public trades (unchanged: global, V1+V2) ----------

    def handle_list_trades(self):
        trades_sorted = sorted(TRADES, key=lambda t: int(t["timestamp"]), reverse=True)

        trades_payload = []
        for t in trades_sorted:
            trades_payload.append({
                "trade_id": str(t["trade_id"]),
                "buyer_id": str(t["buyer_id"]),
                "seller_id": str(t["seller_id"]),
                "price": int(t["price"]),
                "quantity": int(t["quantity"]),
                "timestamp": int(t["timestamp"]),
            })

        self._send_gbuf(200, {"trades": trades_payload})

    # ---------- NEW: public V2-only trades for a contract ----------

    def handle_v2_trades(self, parsed):
        qs = parse_qs(parsed.query)
        if "delivery_start" not in qs or "delivery_end" not in qs:
            self._send_no_content(400)
            return

        try:
            delivery_start = int(qs["delivery_start"][0])
            delivery_end = int(qs["delivery_end"][0])
        except Exception:
            self._send_no_content(400)
            return

        HOUR_MS = 3600000
        if (delivery_start % HOUR_MS) != 0 or (delivery_end % HOUR_MS) != 0:
            self._send_no_content(400)
            return
        if delivery_end <= delivery_start or delivery_end - delivery_start != HOUR_MS:
            self._send_no_content(400)
            return

        # Filter only V2 trades for the given contract
        v2_trades = [
            t for t in TRADES
            if t.get("source") == "v2"
            and t.get("delivery_start") == delivery_start
            and t.get("delivery_end") == delivery_end
        ]

        v2_trades.sort(key=lambda t: int(t["timestamp"]), reverse=True)

        trades_payload = []
        for t in v2_trades:
            trades_payload.append({
                "trade_id": str(t["trade_id"]),
                "buyer_id": str(t["buyer_id"]),
                "seller_id": str(t["seller_id"]),
                "price": int(t["price"]),
                "quantity": int(t["quantity"]),
                "delivery_start": int(t["delivery_start"]),
                "delivery_end": int(t["delivery_end"]),
                "timestamp": int(t["timestamp"]),
            })

        self._send_gbuf(200, {"trades": trades_payload})

    # ---------- V1 take order (creates trades, updates balance) ----------

    def handle_take_order(self):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        order_id = (data.get("order_id") or "").strip()
        if not order_id:
            self._send_no_content(400)
            return

        order = None
        for o in ORDERS:
            if o.get("order_id") == order_id and o.get("active", True):
                order = o
                break

        if not order:
            self._send_no_content(404)
            return

        order["active"] = False

        trade_id = uuid.uuid4().hex
        now_ms = int(time.time() * 1000)

        trade = {
            "trade_id": trade_id,
            "buyer_id": username,
            "seller_id": order["seller_id"],
            "price": int(order["price"]),
            "quantity": int(order["quantity"]),
            "timestamp": now_ms,
            "delivery_start": int(order["delivery_start"]),
            "delivery_end": int(order["delivery_end"]),
            "source": "v1",  # mark as V1 trade
        }
        TRADES.append(trade)

        self._apply_trade_balances(username, order["seller_id"], int(order["price"]), int(order["quantity"]))

        self._send_gbuf(200, {"trade_id": trade_id})

    # ---------- collateral endpoints ----------

    def handle_set_collateral(self, username: str):
        # Admin-only: Authorization: Bearer password123
        auth = self.headers.get("Authorization") or ""
        if not auth.startswith("Bearer "):
            self._send_no_content(401)
            return
        token = auth[7:].strip()
        if token != "password123":
            self._send_no_content(401)
            return

        if username not in USERS:
            self._send_no_content(404)
            return

        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        if "collateral" not in data:
            self._send_no_content(400)
            return

        try:
            collateral_value = int(data.get("collateral"))
        except Exception:
            self._send_no_content(400)
            return

        COLLATERAL[username] = collateral_value
        self._send_no_content(204)

    def handle_get_balance(self):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        if username not in USERS:
            self._send_no_content(404)
            return

        balance = BALANCES.get(username, 0)
        potential = self._compute_potential_balance(username)
        collateral = COLLATERAL.get(username)
        if collateral is None:
            # unlimited collateral – represent as a very large int
            collateral = 9223372036854775807  # 2^63 - 1

        self._send_gbuf(200, {
            "balance": int(balance),
            "potential_balance": int(potential),
            "collateral": int(collateral),
        })

    # ---------- WebSocket trade stream endpoint ----------

    def handle_trades_stream(self):
        # Basic WebSocket handshake (RFC 6455)
        upgrade = (self.headers.get("Upgrade") or "").lower()
        connection = (self.headers.get("Connection") or "").lower()
        key = self.headers.get("Sec-WebSocket-Key")

        if upgrade != "websocket" or "upgrade" not in connection or not key:
            self.send_response(400)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        accept_src = (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
        accept = base64.b64encode(hashlib.sha1(accept_src).digest()).decode("ascii")

        self.send_response(101, "Switching Protocols")
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", accept)
        self.end_headers()

        # Mark this handler as websocket so finish() doesn't close it
        self._is_websocket = True

        # Store the raw socket for broadcasting
        TRADE_STREAM_CLIENTS.append(self.request)
        # We don't read frames; stream is server -> client only.
        # When the client disconnects, send will fail and we drop it.

    # ---------- Bulk operations ----------

    def handle_bulk_operations(self):
        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        contracts = data.get("contracts")
        if not isinstance(contracts, list):
            self._send_no_content(400)
            return

        results = []

        for contract in contracts:
            try:
                delivery_start = int(contract.get("delivery_start"))
                delivery_end = int(contract.get("delivery_end"))
            except Exception:
                self._send_no_content(400)
                return

            HOUR_MS = 3600000
            if (delivery_start % HOUR_MS) != 0 or (delivery_end % HOUR_MS) != 0:
                self._send_no_content(400)
                return
            if delivery_end <= delivery_start or delivery_end - delivery_start != HOUR_MS:
                self._send_no_content(400)
                return

            if not self._check_trading_window(delivery_start):
                # _check_trading_window already sent 425/451
                return

            ops = contract.get("operations")
            if not isinstance(ops, list):
                self._send_no_content(400)
                return

            for op in ops:
                op_type = (op.get("type") or "").strip()
                participant_token = (op.get("participant_token") or "").strip()
                if not op_type or not participant_token:
                    self._send_no_content(400)
                    return

                username = TOKENS.get(participant_token)
                if not username:
                    self._send_no_content(401)
                    return

                if op_type == "create":
                    side = (op.get("side") or "").strip()
                    execution_type = (op.get("execution_type") or "GTC").strip() or "GTC"
                    try:
                        price = int(op.get("price"))
                        quantity = int(op.get("quantity"))
                    except Exception:
                        self._send_no_content(400)
                        return

                    res = self._v2_create_core(
                        username=username,
                        side=side,
                        price=price,
                        quantity=quantity,
                        delivery_start=delivery_start,
                        delivery_end=delivery_end,
                        execution_type=execution_type,
                    )
                    if res is None:
                        # Error already sent
                        return
                    results.append({
                        "type": "create",
                        "order_id": res["order_id"],
                        "status": res["status"],
                    })

                elif op_type == "modify":
                    order_id = (op.get("order_id") or "").strip()
                    if not order_id:
                        self._send_no_content(400)
                        return
                    try:
                        price = int(op.get("price"))
                        quantity = int(op.get("quantity"))
                    except Exception:
                        self._send_no_content(400)
                        return

                    res = self._v2_modify_core(username, order_id, price, quantity)
                    if res is None:
                        return
                    results.append({
                        "type": "modify",
                        "order_id": res["order_id"],
                    })

                elif op_type == "cancel":
                    order_id = (op.get("order_id") or "").strip()
                    if not order_id:
                        self._send_no_content(400)
                        return

                    ok = self._v2_cancel_core(username, order_id)
                    if not ok:
                        return
                    results.append({
                        "type": "cancel",
                        "order_id": order_id,
                    })

                else:
                    self._send_no_content(400)
                    return

        self._send_gbuf(200, {"results": results})


def run():
    server = HTTPServer(("", 8080), Handler)
    print("Server running on port 8080...")
    server.serve_forever()


if __name__ == "__main__":
    run()
