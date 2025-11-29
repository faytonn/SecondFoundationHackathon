from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from galacticbuffer import encode_message, decode_message
import uuid
import time

USERS = {}    # username -> password
TOKENS = {}   # token -> username

# Each order:
# {
#   "order_id": str,
#   "seller_id": str,
#   "price": int,
#   "quantity": int,
#   "delivery_start": int,  # unix ms
#   "delivery_end": int,    # unix ms
#   "active": bool
# }
ORDERS = []

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
        elif self.path == "/trades":
            self.handle_take_order()
        else:
            self.send_response(404)
            self.end_headers()


    # -- forgot password --
    def do_PUT(self):
        if self.path == "/user/password":
            self.handle_change_password()
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


    def handle_change_password(self):
        # Request body (GalacticBuf):
        #   username (string)
        #   old_password (string)
        #   new_password (string)

        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            # invalid GalacticBuf
            self._send_no_content(400)
            return

        username = (data.get("username") or "").strip()
        old_password = (data.get("old_password") or "").strip()
        new_password = (data.get("new_password") or "").strip()

        # 400 Bad Request – missing/empty fields
        if not username or not old_password or not new_password:
            self._send_no_content(400)
            return

        user_password = USERS.get(username)

        # 401 Unauthorized – user not found
        if user_password is None:
            self._send_no_content(401)
            return

        # 401 Unauthorized – old password doesn't match
        if user_password != old_password:
            self._send_no_content(401)
            return

        try:
            # Update password
            USERS[username] = new_password

            # Invalidate all existing tokens for this user
            tokens_to_delete = [
                token for token, u in TOKENS.items() if u == username
            ]
            for token in tokens_to_delete:
                del TOKENS[token]

        except Exception:
            # Any unexpected error → 500
            self._send_no_content(500)
            return

        # 204 No Content – success
        self._send_no_content(204)
    # ---------- /orders (GET) ----------

    def handle_list_orders(self, parsed):
        qs = parse_qs(parsed.query)

        # Both delivery_start and delivery_end are required
        if "delivery_start" not in qs or "delivery_end" not in qs:
            self._send_no_content(400)
            return

        try:
            delivery_start = int(qs["delivery_start"][0])
            delivery_end = int(qs["delivery_end"][0])
        except Exception:
            self._send_no_content(400)
            return

        # Only active orders that match this contract window
        matching = [
            o for o in ORDERS
            if o.get("active", True)
            and int(o.get("delivery_start", 0)) == delivery_start
            and int(o.get("delivery_end", 0)) == delivery_end
        ]

        # Sort by price ascending (cheapest first)
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

    # ---------- /orders (POST) ----------

    def handle_submit_order(self):
        # Requires authentication
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

        # quantity must be positive
        if quantity <= 0:
            self._send_no_content(400)
            return

        # Delivery times aligned to 1-hour boundaries and exactly 1 hour apart
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

    # ---------- /trades (GET) ----------

    def handle_list_trades(self):
        # trades sorted by timestamp descending (newest first)
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

    # ---------- /trades (POST) ----------

       # ---------- /trades (POST) ----------
    def handle_take_order(self):
        # Requires authentication
        username = self._get_authenticated_user()
        if not username:
            # 401 Unauthorized – no valid token
            self._send_no_content(401)
            return

        try:
            raw = self._read_body()
            data = decode_message(raw)
        except Exception:
            # Bad GalacticBuf → treat as bad request
            self._send_no_content(400)
            return

        order_id = (data.get("order_id") or "").strip()
        if not order_id:
            # 400 Bad Request – order_id missing/empty
            self._send_no_content(400)
            return

        # Find active order by id
        order = None
        for o in ORDERS:
            if o.get("order_id") == order_id and o.get("active", True):
                order = o
                break

        if not order:
            # 404 Not Found – order doesn't exist or not active
            self._send_no_content(404)
            return

        # Mark order as FILLED / inactive
        order["active"] = False

        # Create trade record
        import time
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

        # 200 OK with GalacticBuf { trade_id }
        self._send_gbuf(200, {"trade_id": trade_id})


def run():
    server = HTTPServer(("", 8080), Handler)
    print("Server running on port 8080...")
    server.serve_forever()


if __name__ == "__main__":
    run()
