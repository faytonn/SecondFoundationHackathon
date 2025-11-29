from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import struct
import uuid
import time

def _u8(b, i):
    return b[i], i+1

def _u16(b, i):
    return struct.unpack_from(">H", b, i)[0], i+2

def _u32(b, i):
    return struct.unpack_from(">I", b, i)[0], i+4

def _bs(b, i, n):
    return b[i:i+n], i+n

def _str_v1(b, i):
    ln, i = _u16(b, i)
    s, i = _bs(b, i, ln)
    return s.decode(), i

def _str_v2(b, i):
    ln, i = _u32(b, i)
    s, i = _bs(b, i, ln)
    return s.decode(), i

def _field_v1(b, i):
    k, i = _str_v1(b, i)
    t, i = _u8(b, i)
    if t == 1:
        v, i = _str_v1(b, i)
        return k, v, i
    if t == 2:
        n, i = _u16(b, i)
        out = []
        for _ in range(n):
            s, i = _str_v1(b, i)
            out.append(s)
        return k, out, i
    if t == 3:
        v, i = _u32(b, i)
        return k, v, i
    if t == 4:
        v, i = _u16(b, i)
        return k, v, i
    return k, None, i

def _field_v2(b, i):
    k, i = _str_v2(b, i)
    t, i = _u8(b, i)
    if t == 1:
        v, i = _str_v2(b, i)
        return k, v, i
    if t == 2:
        n, i = _u32(b, i)
        out = []
        for _ in range(n):
            s, i = _str_v2(b, i)
            out.append(s)
        return k, out, i
    if t == 3:
        v, i = _u32(b, i)
        return k, v, i
    if t == 4:
        v, i = _u16(b, i)
        return k, v, i
    if t == 5:
        ln, i = _u32(b, i)
        v, i = _bs(b, i, ln)
        return k, v, i
    return k, None, i

def decode_message(b):
    if b[0] == 1:
        return decode_v1(b)
    return decode_v2(b)

def decode_v1(b):
    i = 0
    _, i = _u8(b, i)
    fc, i = _u8(b, i)
    _, i = _u16(b, i)
    out = {}
    for _ in range(fc):
        k, v, i = _field_v1(b, i)
        out[k] = v
    return out

def decode_v2(b):
    i = 0
    _, i = _u8(b, i)
    fc, i = _u8(b, i)
    _, i = _u32(b, i)
    out = {}
    for _ in range(fc):
        k, v, i = _field_v2(b, i)
        out[k] = v
    return out

def _e8(x):
    return struct.pack("B", x)

def _e32(x):
    return struct.pack(">I", x)

def _estr(s):
    b = s.encode()
    return _e32(len(b)) + b

def _ef(name, v):
    out = _estr(name)
    if isinstance(v, str):
        out += _e8(1) + _estr(v)
    elif isinstance(v, list):
        out += _e8(2) + _e32(len(v))
        for s in v:
            out += _estr(s)
    elif isinstance(v, int):
        out += _e8(3) + _e32(v)
    elif isinstance(v, bytes):
        out += _e8(5) + _e32(len(v)) + v
    else:
        out += _e8(1) + _estr(str(v))
    return out

def encode_message(obj):
    body = b""
    for k, v in obj.items():
        body += _ef(k, v)
    fc = len(obj)
    h = b"\x02" + _e8(fc) + _e32(len(body) + 6)
    return h + body


USERS = {}
TOKENS = {}
ORDERS = []
V2_ORDERS = []
TRADES = []


class Handler(BaseHTTPRequestHandler):
    def _read_body(self):
        l = int(self.headers.get("Content-Length", "0"))
        if l <= 0:
            return b""
        return self.rfile.read(l)

    def _send_no_content(self, s):
        self.send_response(s)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _send_gbuf(self, s, o):
        b = encode_message(o)
        self.send_response(s)
        self.send_header("Content-Type", "application/x-galacticbuf")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _auth(self):
        a = self.headers.get("Authorization") or ""
        if not a.startswith("Bearer "):
            return None
        t = a[7:].strip()
        return TOKENS.get(t)

    def do_GET(self):
        p = urlparse(self.path)
        if p.path == "/health":
            w = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(w)))
            self.end_headers()
            self.wfile.write(w)
        elif p.path == "/orders":
            self.handle_list_orders(p)
        elif p.path == "/trades":
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
        elif self.path == "/v2/orders":
            self.handle_v2_submit_order()
        else:
            self.send_response(404)
            self.end_headers()

    def do_PUT(self):
        if self.path == "/user/password":
            self.handle_change_password()
        else:
            self.send_response(404)
            self.end_headers()

    def handle_register(self):
        try:
            d = decode_message(self._read_body())
        except:
            self._send_no_content(400)
            return
        u = (d.get("username") or "").strip()
        p = (d.get("password") or "").strip()
        if not u or not p:
            self._send_no_content(400)
            return
        if u in USERS:
            self._send_no_content(409)
            return
        USERS[u] = p
        self._send_no_content(204)

    def handle_login(self):
        try:
            d = decode_message(self._read_body())
        except:
            self._send_no_content(401)
            return
        u = (d.get("username") or "").strip()
        p = (d.get("password") or "").strip()
        if not u or not p:
            self._send_no_content(401)
            return
        if USERS.get(u) != p:
            self._send_no_content(401)
            return
        t = uuid.uuid4().hex
        TOKENS[t] = u
        self._send_gbuf(200, {"token": t})

    def handle_change_password(self):
        try:
            d = decode_message(self._read_body())
        except:
            self._send_no_content(400)
            return
        u = (d.get("username") or "").strip()
        op = (d.get("old_password") or "").strip()
        np = (d.get("new_password") or "").strip()
        if not u or not op or not np:
            self._send_no_content(400)
            return
        if USERS.get(u) != op:
            self._send_no_content(401)
            return
        USERS[u] = np
        r = [k for k,v in TOKENS.items() if v == u]
        for k in r:
            del TOKENS[k]
        self._send_no_content(204)

    def handle_list_orders(self, p):
        q = parse_qs(p.query)
        if "delivery_start" not in q or "delivery_end" not in q:
            self._send_no_content(400)
            return
        try:
            ds = int(q["delivery_start"][0])
            de = int(q["delivery_end"][0])
        except:
            self._send_no_content(400)
            return
        m = [o for o in ORDERS if o["active"] and o["delivery_start"]==ds and o["delivery_end"]==de]
        m.sort(key=lambda o:o["price"])
        out = []
        for o in m:
            out.append({
                "order_id": o["order_id"],
                "price": o["price"],
                "quantity": o["quantity"],
                "delivery_start": o["delivery_start"],
                "delivery_end": o["delivery_end"]
            })
        self._send_gbuf(200, {"orders": out})

    def handle_submit_order(self):
        u = self._auth()
        if not u:
            self._send_no_content(401)
            return
        try:
            d = decode_message(self._read_body())
        except:
            self._send_no_content(400)
            return
        try:
            p = int(d.get("price"))
            q = int(d.get("quantity"))
            ds = int(d.get("delivery_start"))
            de = int(d.get("delivery_end"))
        except:
            self._send_no_content(400)
            return
        if q<=0:
            self._send_no_content(400)
            return
        H=3600000
        if ds%H!=0 or de%H!=0 or de-ds!=H:
            self._send_no_content(400)
            return
        oid=uuid.uuid4().hex
        ORDERS.append({
            "order_id":oid,
            "seller_id":u,
            "price":p,
            "quantity":q,
            "delivery_start":ds,
            "delivery_end":de,
            "active":True
        })
        self._send_gbuf(200, {"order_id":oid})

    def handle_v2_submit_order(self):
        u=self._auth()
        if not u:
            self._send_no_content(401)
            return
        try:
            d=decode_message(self._read_body())
        except:
            self._send_no_content(400)
            return
        s=(d.get("side") or "").strip()
        if s not in ("buy","sell"):
            self._send_no_content(400)
            return
        try:
            p=int(d.get("price"))
            q=int(d.get("quantity"))
            ds=int(d.get("delivery_start"))
            de=int(d.get("delivery_end"))
        except:
            self._send_no_content(400)
            return
        if q<=0:
            self._send_no_content(400)
            return
        H=3600000
        if ds%H!=0 or de%H!=0 or de-ds!=H:
            self._send_no_content(400)
            return
        oid=uuid.uuid4().hex
        V2_ORDERS.append({
            "order_id":oid,
            "side":s,
            "user_id":u,
            "price":p,
            "quantity":q,
            "delivery_start":ds,
            "delivery_end":de,
            "status":"ACTIVE"
        })
        self._send_gbuf(200, {"order_id":oid})

    def handle_list_trades(self):
        t=sorted(TRADES,key=lambda x:x["timestamp"],reverse=True)
        out=[]
        for x in t:
            out.append({
                "trade_id":x["trade_id"],
                "buyer_id":x["buyer_id"],
                "seller_id":x["seller_id"],
                "price":x["price"],
                "quantity":x["quantity"],
                "timestamp":x["timestamp"]
            })
        self._send_gbuf(200, {"trades":out})

    def handle_take_order(self):
        u=self._auth()
        if not u:
            self._send_no_content(401)
            return
        try:
            d=decode_message(self._read_body())
        except:
            self._send_no_content(400)
            return
        oid=(d.get("order_id") or "").strip()
        if not oid:
            self._send_no_content(400)
            return
        o=None
        for x in ORDERS:
            if x["order_id"]==oid and x["active"]:
                o=x
                break
        if not o:
            self._send_no_content(404)
            return
        o["active"]=False
        ORDERS.remove(o)
        tid=uuid.uuid4().hex
        ts=int(time.time()*1000)
        TRADES.append({
            "trade_id":tid,
            "buyer_id":u,
            "seller_id":o["seller_id"],
            "price":o["price"],
            "quantity":o["quantity"],
            "timestamp":ts
        })
        self._send_gbuf(200, {"trade_id":tid})


def run():
    s=HTTPServer(("",8080),Handler)
    print("Server running on port 8080...")
    s.serve_forever()

if __name__=="__main__":
    run()
