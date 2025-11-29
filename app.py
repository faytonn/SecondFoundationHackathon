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

# New: balances + collateral
BALANCES = {}     # username -> int
COLLATERAL = {}   # username -> collateral limit (None = unlimited)

# New: DNA samples: username -> list of normalized DNA strings
DNA_SAMPLES = {}


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
        Potential balance = current balance + effect if all ACTIVE V2 orders fill.
        Buy -> pays price * qty (subtract)
        Sell -> receives price * qty (add)
        """
        balance = BALANCES.get(username, 0)
        for o in V2_ORDERS:
            if o.get("owner") != username:
                continue
            if o.get("status") != "ACTIVE":
                continue
            qty = int(o.get("quantity", 0))
            if qty <= 0:
                continue
            price = int(o["price"])
            if o["side"] == "buy":
                balance -= price * qty
            else:
                balance += price * qty
        return balance

    # ---------- DNA helpers ----------

    def _normalize_and_validate_dna(self, dna: str):
        """
        Strip + uppercase, ensure only C/G/A/T and length divisible by 3.
        Return normalized DNA string or None if invalid.
        """
        s = (dna or "").strip().upper()
        if not s:
            return None
        if len(s) % 3 != 0:
            return None
        for ch in s:
            if ch not in ("C", "G", "A", "T"):
                return None
        return s

    def _codon_distance(self, ref: str, sample: str, max_diff: int) -> int:
        """
        Levenshtein distance on codon sequences (3-char chunks).
        Operations: insert, delete, substitute codons.
        Early-stops if distance exceeds max_diff.
        """
        ref_codons = [ref[i:i+3] for i in range(0, len(ref), 3)]
        sam_codons = [sample[i:i+3] for i in range(0, len(sample), 3)]

        n = len(ref_codons)
        m = len(sam_codons)

        # Quick lower bound
        if abs(n - m) > max_diff:
            return max_diff + 1

        # Standard Levenshtein DP with early cutoff
        prev = list(range(m + 1))
        curr = [0] * (m + 1)

        for i in range(1, n + 1):
            curr[0] = i
            row_min = curr[0]
            rc = ref_codons[i - 1]
            for j in range(1, m + 1):
                sc = sam_codons[j - 1]
                cost = 0 if rc == sc else 1

                deletion = prev[j] + 1
                insertion = curr[j - 1] + 1
                substitution = prev[j - 1] + cost

                v = deletion
                if insertion < v:
                    v = insertion
                if substitution < v:
                    v = substitution
                curr[j] = v

                if v < row_min:
                    row_min = v

            if row_min > max_diff:
                return max_diff + 1

            prev, curr = curr, prev

        return prev[m]

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
            # Not implemented yet
            self.send_response(501)
            self.send_header("Content-Length", "0")
            self.end_headers()
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

        if user
