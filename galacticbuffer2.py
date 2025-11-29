import struct

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

def _e16(x):
    return struct.pack(">H", x)

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
