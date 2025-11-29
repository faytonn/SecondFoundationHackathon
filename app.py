from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from galacticbuffer import encode_message, decode_message
import uuid, time, base64, hashlib

USERS, TOKENS, ORDERS, V2_ORDERS, TRADES, BALANCES, COLLATERAL, DNA_SAMPLES = {}, {}, [], [], [], {}, {}, {}
TRADE_STREAM_CLIENTS, ORDER_BOOK_STREAM_CLIENTS, EXECUTION_REPORT_CLIENTS = [], [], {}

class Handler(BaseHTTPRequestHandler):
    def _check_trading_window(self, delivery_start: int):
        now_ms = int(time.time() * 1000)
        return open_time := delivery_start - 15 * 24 * 60 * 60 * 1000, close_time := delivery_start - 60 * 1000

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
        token = self.headers.get("Authorization", "").split("Bearer ")[-1]
        return TOKENS.get(token)

    def _apply_trade_balances(self, buyer_id: str, seller_id: str, price: int, quantity: int):
        amount = price * quantity
        BALANCES[buyer_id] = BALANCES.get(buyer_id, 0) - amount
        BALANCES[seller_id] = BALANCES.get(seller_id, 0) + amount

    def _compute_potential_balance(self, username: str) -> int:
        balance = BALANCES.get(username, 0)
        for o in V2_ORDERS:
            if o.get("owner") == username and o.get("status") == "ACTIVE":
                qty, price = int(o.get("quantity", 0)), int(o.get("price", 0))
                balance += price * qty if o["side"] == "sell" else -price * qty
        return balance

    def _validate_dna_sample(self, dna: str) -> bool:
        return len(dna) % 3 == 0 and all(ch in "CGA" for ch in dna)

    def _split_codons(self, dna: str):
        return [dna[i:i+3] for i in range(0, len(dna), 3)]

    def _codon_edit_distance_bounded(self, ref_codons, sample_codons, max_diff: int) -> int:
        if max_diff < 0: return max_diff + 1
        if abs(len(ref_codons) - len(sample_codons)) > max_diff: return max_diff + 1
        prev = {0: 0}
        for i in range(1, len(ref_codons) + 1):
            curr = {j: min(prev.get(j, max_diff+1) + 1, prev.get(j-1, max_diff+1) + (ref_codons[i-1] != sample_codons[j-1])) for j in range(max(0, i - max_diff), min(len(sample_codons), i + max_diff) + 1)}
            if min(curr.values()) > max_diff: return max_diff + 1
            prev = curr
        return prev.get(len(sample_codons), max_diff + 1)

    def _dna_matches(self, reference: str, submitted: str) -> bool:
        ref_codons, sub_codons = self._split_codons(reference), self._split_codons(submitted)
        return self._codon_edit_distance_bounded(ref_codons, sub_codons, len(ref_codons) // 100000) <= len(ref_codons) // 100000

    def _ws_build_binary_frame(self, payload: bytes) -> bytes:
        length = len(payload)
        header = bytes([0x82, 126]) + length.to_bytes(2, "big") if length < (1 << 16) else bytes([0x82, 127]) + length.to_bytes(8, "big")
        return header + payload

    def _send_frame(self, sock, data: dict):
        try:
            sock.sendall(self._ws_build_binary_frame(encode_message(data)))
        except Exception: pass

    def _handle_websocket_stream(self, client_list):
        try:
            while True: self.request.recv(1024)
        except Exception: pass
        finally: client_list.remove(self.request)

    def handle_register(self):
        data = self._parse_request_body()
        if not data or data.get("username") in USERS: return self._send_no_content(409)
        USERS[data["username"]] = data["password"]
        self._send_no_content(204)

    def handle_login(self):
        data = self._parse_request_body()
        username, password = data.get("username"), data.get("password")
        if USERS.get(username) != password: return self._send_no_content(401)
        token = uuid.uuid4().hex
        TOKENS[token] = username
        self._send_gbuf(200, {"token": token})

    def handle_trade_stream(self):
        if not self._check_websocket(): return
        TRADE_STREAM_CLIENTS.append(self.request)
        self._handle_websocket_stream(TRADE_STREAM_CLIENTS)

    def handle_order_book_stream(self):
        if not self._check_websocket(): return
        ORDER_BOOK_STREAM_CLIENTS.append(self.request)
        self._handle_websocket_stream(ORDER_BOOK_STREAM_CLIENTS)

    def handle_execution_reports_stream(self, parsed):
        if not self._check_websocket(): return
        token = parse_qs(parsed.query).get("token", [None])[0]
        if not TOKENS.get(token): self._send_no_content(401)
        EXECUTION_REPORT_CLIENTS.setdefault(TOKENS[token], []).append(self.request)
        self._handle_websocket_stream(EXECUTION_REPORT_CLIENTS[TOKENS[token]])

    def _check_websocket(self):
        if self.command != "GET" or "websocket" not in self.headers.get("Upgrade", "").lower(): return self._send_no_content(400)
        key = self.headers.get("Sec-WebSocket-Key")
        accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
        self.send_response(101, "Switching Protocols")
        self.send_header("Upgrade", "websocket")
        self.send_header("Sec-WebSocket-Accept", accept)
        self.end_headers()
        return True

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_no_content(200)
        elif parsed.path == "/orders":
            self._send_gbuf(200, {"orders": self._get_orders(parsed)})
        else:
            self.send_response(404)

    def do_POST(self):
        if self.path == "/register": self.handle_register()
        elif self.path == "/login": self.handle_login()
        elif self.path == "/orders": self.handle_submit_order()
        elif self.path == "/v2/orders": self.handle_submit_order_v2()
        else: self._send_no_content(404)

    def _parse_request_body(self):
        try: return decode_message(self._read_body())
        except Exception: return {}

    def _get_orders(self, parsed):
        qs = parse_qs(parsed.query)
        return [o for o in ORDERS if int(o.get("delivery_start", 0)) == int(qs["delivery_start"][0])]

def run():
    server = ThreadingHTTPServer(("", 8080), Handler)
    print("Server running on port 8080...")
    server.serve_forever()

if __name__ == "__main__":
    run()
