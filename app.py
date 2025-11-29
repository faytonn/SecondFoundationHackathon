from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from galacticbuffer import encode_message, decode_message
import uuid
import time

USERS = {}
TOKENS = {}

ORDERS = []
V2_ORDERS = []
TRADES = []

# username -> list of DNA samples (strings)
DNA_DB = {}


# ---------- DNA HELPERS (shared) ----------

def is_valid_dna(seq: str) -> bool:
    if not seq:
        return False
    if len(seq) % 3 != 0:
        return False
    allowed_chars = {"C", "G", "A", "T"}
    return all(c in allowed_chars for c in seq)


def codon_edit_distance(a: str, b: str) -> int:
    """
    Compute Levenshtein distance between two DNA sequences
    at the codon level (3-character groups).
    Operations: insertion, deletion, substitution (cost=1).
    """
    codons_a = [a[i:i+3] for i in range(0, len(a), 3)]
    codons_b = [b[i:i+3] for i in range(0, len(b), 3)]

    n = len(codons_a)
    m = len(codons_b)

    prev = list(range(m + 1))
    curr = [0] * (m + 1)

    for i in range(1, n + 1):
        curr[0] = i
        ca = codons_a[i - 1]
        for j in range(1, m + 1):
            cb = codons_b[j - 1]
            cost = 0 if ca == cb else 1
            curr[j] = min(
                prev[j] + 1,       # deletion
                curr[j - 1] + 1,   # insertion
                prev[j - 1] + cost # substitution
            )
        prev, curr = curr, prev

    return prev[m]


class Handler(BaseHTTPRequestHandler):
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

    # ---------- HTTP METHODS ----------

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

    # ---------- AUTH & USERS ----------

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

    # ---------- V1 ORDERS ----------

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

    # ---------- V2: SUBMIT + MATCHING ----------

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
        try:
            price = int(data.get("price"))
            quantity = int(data.get("quantity"))
            delivery_start = int(data.get("delivery_start"))
            delivery_end = int(data.get("delivery_end"))
        except Exception:
            self._send_no_content(400)
            return

        if side not in ("buy", "sell"):
            self._send_no_content(400)
            return

        if quantity <= 0:
            self._send_no_content(400)
            return

        HOUR_MS = 3600000
        if (delivery_start % HOUR_MS) != 0 or (delivery_end % HOUR_MS) != 0:
            self._send_no_content(400)
            return
        if delivery_end <= delivery_start or delivery_end - delivery_start != HOUR_MS:
            self._send_no_content(400)
            return

        order_id = uuid.uuid4().hex
        now_ms = int(time.time() * 1000)

        remaining = quantity
        filled_quantity = 0

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
            candidates.sort(key=lambda o: (o["price"], o.get("created_at", 0)))
        else:
            candidates = [
                o for o in V2_ORDERS
                if o.get("status") == "ACTIVE"
                and o["side"] == "buy"
                and o["delivery_start"] == delivery_start
                and o["delivery_end"] == delivery_end
                and o["quantity"] > 0
                and o["price"] >= price
            ]
            candidates.sort(key=lambda o: (-o["price"], o.get("created_at", 0)))

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

            trade_price = resting["price"]
            trade_id = uuid.uuid4().hex
            trade_ts = int(time.time() * 1000)

            TRADES.append({
                "trade_id": trade_id,
                "buyer_id": buyer_id,
                "seller_id": seller_id,
                "price": trade_price,
                "quantity": trade_qty,
                "timestamp": trade_ts,
            })

            remaining -= trade_qty
            filled_quantity += trade_qty

            resting["quantity"] -= trade_qty
            if resting["quantity"] <= 0:
                resting["quantity"] = 0
                resting["status"] = "FILLED"

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

        self._send_gbuf(200, {
            "order_id": order_id,
            "status": status,
            "filled_quantity": filled_quantity,
        })

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

        if new_quantity <= 0:
            self._send_no_content(400)
            return

        order = None
        for o in V2_ORDERS:
            if o.get("order_id") == order_id:
                order = o
                break

        if not order or order.get("status", "ACTIVE") != "ACTIVE" or order["quantity"] <= 0:
            self._send_no_content(404)
            return

        if order.get("owner") != username:
            self._send_no_content(403)
            return

        old_price = order["price"]
        old_quantity = order["quantity"]

        order["price"] = new_price
        order["quantity"] = new_quantity

        now_ms = int(time.time() * 1000)
        if new_price != old_price or new_quantity > old_quantity:
            order["created_at"] = now_ms

        side = order["side"]
        delivery_start = order["delivery_start"]
        delivery_end = order["delivery_end"]

        remaining = order["quantity"]
        filled_quantity = 0

        if side == "buy":
            candidates = [
                o for o in V2_ORDERS
                if o.get("status", "ACTIVE") == "ACTIVE"
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
                if o.get("status", "ACTIVE") == "ACTIVE"
                and o["side"] == "buy"
                and o["delivery_start"] == delivery_start
                and o["delivery_end"] == delivery_end
                and o["quantity"] > 0
                and o["order_id"] != order_id
                and o["price"] >= new_price
            ]
            candidates.sort(key=lambda o: (-o["price"], o.get("created_at", 0)))

        for resting in candidates:
            if remaining <= 0:
                break
            if resting.get("status", "ACTIVE") != "ACTIVE":
                continue
            if resting["quantity"] <= 0:
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

            TRADES.append({
                "trade_id": trade_id,
                "buyer_id": buyer_id,
                "seller_id": seller_id,
                "price": trade_price,
                "quantity": trade_qty,
                "timestamp": ts,
            })

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

        self._send_gbuf(200, {
            "order_id": order["order_id"],
            "status": order["status"],
            "filled_quantity": filled_quantity,
        })

    def handle_cancel_order(self, order_id: str):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        order = None
        for o in V2_ORDERS:
            if o.get("order_id") == order_id:
                order = o
                break

        if not order or order.get("status") != "ACTIVE" or order["quantity"] <= 0:
            self._send_no_content(404)
            return

        if order.get("owner") != username:
            self._send_no_content(403)
            return

        order["status"] = "CANCELLED"
        order["quantity"] = 0

        self._send_no_content(204)

    # ---------- V2 ORDER BOOK & MY ORDERS ----------

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

        bids.sort(key=lambda x: (-x[0]["price"], x[0].get("created_at", 0)))
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

    # ---------- TRADES (V1) ----------

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
        }
        TRADES.append(trade)

        self._send_gbuf(200, {"trade_id": trade_id})

    # ---------- DNA ENDPOINTS ----------

    def handle_dna_submit(self):
        """
        POST /dna-submit
        GalacticBuf body:
          username (string)
          password (string)
          dna_sample (string)

        - 400: invalid input or invalid DNA
        - 401: wrong username/password
        - 204: success
        """
        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            self._send_no_content(400)
            return

        username = (data.get("username") or "").strip()
        password = (data.get("password") or "")
        dna_sample = (data.get("dna_sample") or "").strip()

        if not username or not password or not dna_sample:
            self._send_no_content(400)
            return

        if not is_valid_dna(dna_sample):
            self._send_no_content(400)
            return

        user_password = USERS.get(username)
        if user_password is None:
            self._send_no_content(401)
            return

        if user_password != password:
            self._send_no_content(401)
            return

        if username not in DNA_DB:
            DNA_DB[username] = []
        if dna_sample not in DNA_DB[username]:
            DNA_DB[username].append(dna_sample)

        self._send_no_content(204)

    def handle_dna_login(self):
        """
        POST /dna-login
        GalacticBuf body:
          username (string)
          dna_sample (string)

        - 400: invalid input or invalid DNA
        - 401: user missing / no DNA / no match
        - 200: { token: string }
        """
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

        if not is_valid_dna(dna_sample):
            self._send_no_content(400)
            return

        if username not in USERS:
            self._send_no_content(401)
            return

        registered_samples = DNA_DB.get(username)
        if not registered_samples:
            self._send_no_content(401)
            return

        matched = False

        for ref in registered_samples:
            ref_codons = len(ref) // 3
            allowed_diff = ref_codons // 100000

            # If diff==0 and lengths differ, impossible to match
            if allowed_diff == 0 and len(ref) != len(dna_sample):
                continue

            dist = codon_edit_distance(ref, dna_sample)
            if dist <= allowed_diff:
                matched = True
                break

        if not matched:
            self._send_no_content(401)
            return

        token = uuid.uuid4().hex
        TOKENS[token] = username
        self._send_gbuf(200, {"token": token})


def run():
    server = HTTPServer(("", 8080), Handler)
    print("Server running on port 8080...")
    server.serve_forever()


if __name__ == "__main__":
    run()
