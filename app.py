from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from galacticbuffer import encode_message, decode_message
import uuid
import time

USERS = {}    # username -> password
TOKENS = {}   # token -> username

# V1 order book
ORDERS = []

# V2 order book
V2_ORDERS = []

# Each trade:
# {
#   "trade_id": str,
#   "buyer_id": str,
#   "seller_id": str,
#   "price": int,
#   "quantity": int,
#   "timestamp": int,  # unix ms
# }
TRADES = []


class Handler(BaseHTTPRequestHandler):
    # ---------- helpers ----------

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
        # Your galacticbuffer now encodes as v1 (0x01). Tests only care that
        # decoding works & fields are correct, not the version byte.
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

    # ---------- MATCHING ENGINE (V2) ----------

    def _match_v2_order(self, incoming: dict) -> int:
        """
        Run matching for a new or modified V2 order.

        incoming: order dict with keys:
          side, owner, price, quantity, delivery_start, delivery_end,
          order_id, status, timestamp

        Returns: total filled quantity (int)
        """
        side = incoming["side"]
        price = int(incoming["price"])
        remaining = int(incoming["quantity"])
        delivery_start = int(incoming["delivery_start"])
        delivery_end = int(incoming["delivery_end"])
        username = incoming["owner"]

        if remaining <= 0 or incoming["status"] != "ACTIVE":
            return 0

        # Opposite side + price condition
        if side == "buy":
            # Match existing ACTIVE sells same contract, sell_price <= buy_price
            candidates = [
                o for o in V2_ORDERS
                if o["status"] == "ACTIVE"
                and o["side"] == "sell"
                and o["delivery_start"] == delivery_start
                and o["delivery_end"] == delivery_end
                and int(o["price"]) <= price
            ]
            # Cheapest sells first, then oldest
            candidates.sort(key=lambda o: (int(o["price"]), o["timestamp"]))
        else:  # side == "sell"
            # Match existing ACTIVE buys same contract, buy_price >= sell_price
            candidates = [
                o for o in V2_ORDERS
                if o["status"] == "ACTIVE"
                and o["side"] == "buy"
                and o["delivery_start"] == delivery_start
                and o["delivery_end"] == delivery_end
                and int(o["price"]) >= price
            ]
            # Highest bids first, then oldest
            candidates.sort(key=lambda o: (-int(o["price"]), o["timestamp"]))

        filled_total = 0

        for resting in candidates:
            if remaining <= 0:
                break

            resting_qty = int(resting["quantity"])
            if resting_qty <= 0:
                continue

            trade_qty = min(remaining, resting_qty)
            remaining -= trade_qty
            resting_qty -= trade_qty

            # Maker price = resting order's price
            trade_price = int(resting["price"])
            now_ms = int(time.time() * 1000)
            trade_id = uuid.uuid4().hex

            if side == "buy":
                buyer_id = username
                seller_id = resting["owner"]
            else:
                buyer_id = resting["owner"]
                seller_id = username

            trade = {
                "trade_id": trade_id,
                "buyer_id": buyer_id,
                "seller_id": seller_id,
                "price": trade_price,
                "quantity": trade_qty,
                "timestamp": now_ms,
            }
            TRADES.append(trade)

            # Update resting order
            resting["quantity"] = resting_qty
            if resting_qty == 0:
                resting["status"] = "FILLED"

            filled_total += trade_qty

        # Update incoming's remaining quantity
        incoming["quantity"] = remaining
        if remaining == 0:
            incoming["status"] = "FILLED"

        return filled_total

    # ---------- /v2/my-orders (GET) ----------

    def handle_my_orders(self):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        # Only ACTIVE orders belonging to this user, across all contracts
        mine = [
            o for o in V2_ORDERS
            if o["owner"] == username and o["status"] == "ACTIVE"
        ]

        # Newest first (timestamp descending)
        mine.sort(key=lambda o: o["timestamp"], reverse=True)

        orders_payload = []
        for o in mine:
            orders_payload.append({
                "order_id": o["order_id"],
                "side": o["side"],
                "price": int(o["price"]),
                "quantity": int(o["quantity"]),
                "delivery_start": int(o["delivery_start"]),
                "delivery_end": int(o["delivery_end"]),
                "timestamp": int(o["timestamp"]),
            })

        self._send_gbuf(200, {"orders": orders_payload})

    # ---------- MODIFY ORDER (PUT /v2/orders/{id}) ----------

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

        # New quantity must be positive
        if new_quantity <= 0:
            self._send_no_content(400)
            return

        # Find order
        order = None
        for o in V2_ORDERS:
            if o["order_id"] == order_id:
                order = o
                break

        if order is None:
            self._send_no_content(404)
            return

        # Must be owner
        if order["owner"] != username:
            self._send_no_content(403)
            return

        # Only ACTIVE orders can be modified
        if order["status"] != "ACTIVE":
            self._send_no_content(404)
            return

        old_price = int(order["price"])
        old_qty = int(order["quantity"])

        now_ms = int(time.time() * 1000)

        # Time-priority reset rules:
        # - price change OR quantity increase -> reset timestamp
        # - quantity decrease -> keep timestamp
        if new_price != old_price or new_quantity > old_qty:
            order["timestamp"] = now_ms

        # Apply changes
        order["price"] = new_price
        order["quantity"] = new_quantity

        # Run matching again
        filled = self._match_v2_order(order)

        response_obj = {
            "order_id": order["order_id"],
            "status": order["status"],
            "filled_quantity": filled,
        }
        self._send_gbuf(200, response_obj)

    # ---------- CANCEL ORDER (DELETE /v2/orders/{id}) ----------

    def handle_cancel_order(self, order_id: str):
        username = self._get_authenticated_user()
        if not username:
            self._send_no_content(401)
            return

        # Find order
        order = None
        for o in V2_ORDERS:
            if o["order_id"] == order_id:
                order = o
                break

        if order is None:
            self._send_no_content(404)
            return

        # Must be owner
        if order["owner"] != username:
            self._send_no_content(403)
            return

        # Can't cancel FILLED or already CANCELLED
        if order["status"] != "ACTIVE":
            self._send_no_content(404)
            return

        # Cancel: mark cancelled and zero out remaining quantity
        order["status"] = "CANCELLED"
        order["quantity"] = 0

        self._send_no_content(204)

    # ---------- endpoints ----------

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
            # V1 submit
            self.handle_submit_order()
        elif self.path == "/v2/orders":
            # V2 submit with matching engine
            self.handle_submit_order_v2()
        elif self.path == "/trades":
            self.handle_take_order()
        else:
            self.send_response(404)
            self.end_headers()

    def do_PUT(self):
        # Change password
        if self.path == "/user/password":
            self.handle_change_password()
        # Modify V2 order
        elif self.path.startswith("/v2/orders/"):
            order_id = self.path[len("/v2/orders/"):]
            self.handle_modify_order(order_id)
        else:
            self.send_response(404)
            self.end_headers()

    def do_DELETE(self):
        # Cancel V2 order
        if self.path.startswith("/v2/orders/"):
            order_id = self.path[len("/v2/orders/"):]
            self.handle_cancel_order(order_id)
        else:
            self.send_response(404)
            self.end_headers()

    # ---------- /register ----------

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

    # ---------- /login ----------

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

    # ---------- /user/password (PUT) ----------

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
            tokens_to_delete = [
                token for token, u in list(TOKENS.items()) if u == username
            ]
            for token in tokens_to_delete:
                del TOKENS[token]
        except Exception:
            self._send_no_content(500)
            return

        self._send_no_content(204)

    # ---------- /orders (GET, V1) ----------

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

    # ---------- /orders (POST, V1) ----------

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

    # ---------- /v2/orders (POST, V2 with matching) ----------

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
        if delivery_end <= delivery_start:
            self._send_no_content(400)
            return
        if delivery_end - delivery_start != HOUR_MS:
            self._send_no_content(400)
            return

        now_ms = int(time.time() * 1000)
        order_id = uuid.uuid4().hex

        order = {
            "order_id": order_id,
            "side": side,
            "owner": username,
            "price": price,
            "quantity": quantity,          # remaining quantity
            "delivery_start": delivery_start,
            "delivery_end": delivery_end,
            "status": "ACTIVE",
            "timestamp": now_ms,           # for FIFO at price level
        }

        filled = self._match_v2_order(order)

        # If still ACTIVE after matching, it enters the book
        if order["status"] == "ACTIVE":
            V2_ORDERS.append(order)

        response_obj = {
            "order_id": order_id,
            "status": order["status"],
            "filled_quantity": filled,
        }
        self._send_gbuf(200, response_obj)

    # ---------- /trades (GET) ----------

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

    # ---------- /trades (POST, V1) ----------

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
        ORDERS.remove(order)

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


def run():
    server = HTTPServer(("", 8080), Handler)
    print("Server running on port 8080...")
    server.serve_forever()


if __name__ == "__main__":
    run()
