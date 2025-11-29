import struct

TYPE_INT = 0x01
TYPE_STRING = 0x02
TYPE_LIST = 0x03
TYPE_OBJECT = 0x04
TYPE_BYTES = 0x05  # new in v2


# ---------- ENCODING (v1) ----------

def _encode_int(value: int) -> bytes:
    # 64-bit signed, big-endian
    return struct.pack(">q", value)


def _encode_string_v1(value: str) -> bytes:
    data = value.encode("utf-8")
    if len(data) > 0xFFFF:
        raise ValueError("string too long for v1")
    # 2-byte length
    return struct.pack(">H", len(data)) + data


def _encode_object_v1(obj: dict) -> bytes:
    """
    Encodes an object (type 0x04 value) in v1:
    [field_count][field1][field2]...[fieldN]
    Fields use the same [name_len][name][type][value] format as top-level.
    Only ints and strings inside objects are supported here (as before).
    """
    field_bytes = bytearray()
    field_count = 0

    for name, value in obj.items():
        name_bytes = name.encode("utf-8")
        if not (1 <= len(name_bytes) <= 255):
            raise ValueError("invalid field name length in object")

        field_bytes.append(len(name_bytes))
        field_bytes += name_bytes

        if isinstance(value, int):
            field_bytes.append(TYPE_INT)
            field_bytes += _encode_int(value)

        elif isinstance(value, str):
            field_bytes.append(TYPE_STRING)
            field_bytes += _encode_string_v1(value)

        else:
            # For our use case we only expect ints/strings inside objects
            raise NotImplementedError(f"unsupported object field type for {name!r}")

        field_count += 1

    if field_count > 255:
        raise ValueError("too many fields in object")

    return bytes([field_count]) + field_bytes


def _encode_list_v1(values, elem_type: int) -> bytes:
    if len(values) > 0xFFFF:
        raise ValueError("too many list elements for v1")

    out = bytearray()
    out.append(elem_type)                      # element type
    out += struct.pack(">H", len(values))     # element count (2 bytes)

    if elem_type == TYPE_INT:
        for v in values:
            out += _encode_int(v)
    elif elem_type == TYPE_STRING:
        for v in values:
            out += _encode_string_v1(v)
    elif elem_type == TYPE_OBJECT:
        for v in values:
            if not isinstance(v, dict):
                raise ValueError("list with object type but non-dict value")
            out += _encode_object_v1(v)
    else:
        raise NotImplementedError("unsupported list element type")

    return bytes(out)


def encode_message(fields: dict) -> bytes:
    """
    Encode as GalacticBuf v1 (version 0x01).

    fields: dict like:
      {
        "user_id": 1001,
        "name": "Alice",
        "scores": [100, 200, 300],
        "orders": [ { ... }, { ... } ]
      }
    """
    field_bytes = bytearray()

    for name, value in fields.items():
        name_bytes = name.encode("utf-8")
        if not (1 <= len(name_bytes) <= 255):
            raise ValueError("invalid field name length")

        # field name length + name
        field_bytes.append(len(name_bytes))
        field_bytes += name_bytes

        # type + value
        if isinstance(value, int):
            field_bytes.append(TYPE_INT)
            field_bytes += _encode_int(value)

        elif isinstance(value, str):
            field_bytes.append(TYPE_STRING)
            field_bytes += _encode_string_v1(value)

        elif isinstance(value, list):
            if all(isinstance(v, int) for v in value):
                field_bytes.append(TYPE_LIST)
                field_bytes += _encode_list_v1(value, TYPE_INT)
            elif all(isinstance(v, str) for v in value):
                field_bytes.append(TYPE_LIST)
                field_bytes += _encode_list_v1(value, TYPE_STRING)
            elif all(isinstance(v, dict) for v in value):
                field_bytes.append(TYPE_LIST)
                field_bytes += _encode_list_v1(value, TYPE_OBJECT)
            else:
                raise NotImplementedError("mixed-type lists not supported")

        elif isinstance(value, dict):
            # single nested object
            field_bytes.append(TYPE_OBJECT)
            field_bytes += _encode_object_v1(value)

        elif isinstance(value, (bytes, bytearray)):
            # we *could* support bytes encoding as v2-only, but our app never sends bytes
            # so for now we avoid emitting TYPE_BYTES to keep v1 simple
            raise NotImplementedError("bytes encoding not used in responses")

        else:
            raise NotImplementedError(f"unsupported type for field {name!r}: {type(value)}")

    version = 0x01
    field_count = len(fields)
    total_length = 4 + len(field_bytes)  # header (4) + payload
    if total_length > 0xFFFF:
        raise ValueError("message too big for v1")

    header = struct.pack(">BBH", version, field_count, total_length)
    return header + field_bytes


# ---------- DECODING HELPERS (shared) ----------

def _decode_object_v1(data: bytes, offset: int):
    """Decode an object (type 0x04) using v1 sizes (2-byte string lengths)."""
    if offset >= len(data):
        raise ValueError("truncated object (field count)")
    field_count = data[offset]
    offset += 1

    obj = {}

    for _ in range(field_count):
        if offset >= len(data):
            raise ValueError("truncated object (field name len)")

        name_len = data[offset]
        offset += 1

        if offset + name_len > len(data):
            raise ValueError("truncated object (field name)")

        name = data[offset:offset + name_len].decode("utf-8")
        offset += name_len

        if offset >= len(data):
            raise ValueError("truncated object (type id)")

        type_id = data[offset]
        offset += 1

        if type_id == TYPE_INT:
            if offset + 8 > len(data):
                raise ValueError("truncated object int")
            value = struct.unpack(">q", data[offset:offset + 8])[0]
            offset += 8

        elif type_id == TYPE_STRING:
            if offset + 2 > len(data):
                raise ValueError("truncated object string len")
            str_len = struct.unpack(">H", data[offset:offset + 2])[0]
            offset += 2
            if offset + str_len > len(data):
                raise ValueError("truncated object string data")
            value = data[offset:offset + str_len].decode("utf-8")
            offset += str_len

        else:
            raise NotImplementedError("objects with nested lists/objects/bytes not implemented (v1)")

        obj[name] = value

    return obj, offset


def _decode_object_v2(data: bytes, offset: int):
    """Decode an object (type 0x04) using v2 sizes (4-byte string lengths)."""
    if offset >= len(data):
        raise ValueError("truncated object (field count)")
    field_count = data[offset]
    offset += 1

    obj = {}

    for _ in range(field_count):
        if offset >= len(data):
            raise ValueError("truncated object (field name len)")

        name_len = data[offset]
        offset += 1

        if offset + name_len > len(data):
            raise ValueError("truncated object (field name)")

        name = data[offset:offset + name_len].decode("utf-8")
        offset += name_len

        if offset >= len(data):
            raise ValueError("truncated object (type id)")

        type_id = data[offset]
        offset += 1

        if type_id == TYPE_INT:
            if offset + 8 > len(data):
                raise ValueError("truncated object int")
            value = struct.unpack(">q", data[offset:offset + 8])[0]
            offset += 8

        elif type_id == TYPE_STRING:
            if offset + 4 > len(data):
                raise ValueError("truncated object string len (v2)")
            str_len = struct.unpack(">I", data[offset:offset + 4])[0]
            offset += 4
            if offset + str_len > len(data):
                raise ValueError("truncated object string data (v2)")
            value = data[offset:offset + str_len].decode("utf-8")
            offset += str_len

        elif type_id == TYPE_BYTES:
            if offset + 4 > len(data):
                raise ValueError("truncated object bytes len (v2)")
            b_len = struct.unpack(">I", data[offset:offset + 4])[0]
            offset += 4
            if offset + b_len > len(data):
                raise ValueError("truncated object bytes data (v2)")
            value = data[offset:offset + b_len]
            offset += b_len

        else:
            raise NotImplementedError("objects with nested lists/objects not implemented (v2)")

        obj[name] = value

    return obj, offset


# ---------- DECODING v1 ----------

def _decode_message_v1(data: bytes) -> dict:
    if len(data) < 4:
        raise ValueError("message too short for v1")

    version, field_count, total_len = struct.unpack(">BBH", data[:4])
    if version != 0x01:
        raise ValueError(f"v1 decoder got wrong version {version}")

    offset = 4
    result = {}

    for _ in range(field_count):
        if offset >= len(data):
            raise ValueError("truncated message (field name length)")

        name_len = data[offset]
        offset += 1

        if offset + name_len > len(data):
            raise ValueError("truncated message (field name)")

        name = data[offset:offset + name_len].decode("utf-8")
        offset += name_len

        if offset >= len(data):
            raise ValueError("truncated message (type id)")

        type_id = data[offset]
        offset += 1

        if type_id == TYPE_INT:
            if offset + 8 > len(data):
                raise ValueError("truncated int value")
            value = struct.unpack(">q", data[offset:offset + 8])[0]
            offset += 8

        elif type_id == TYPE_STRING:
            if offset + 2 > len(data):
                raise ValueError("truncated string length")
            str_len = struct.unpack(">H", data[offset:offset + 2])[0]
            offset += 2
            if offset + str_len > len(data):
                raise ValueError("truncated string data")
            value = data[offset:offset + str_len].decode("utf-8")
            offset += str_len

        elif type_id == TYPE_LIST:
            if offset + 3 > len(data):
                raise ValueError("truncated list header")
            elem_type = data[offset]
            offset += 1
            count = struct.unpack(">H", data[offset:offset + 2])[0]
            offset += 2

            items = []
            if elem_type == TYPE_INT:
                for _ in range(count):
                    if offset + 8 > len(data):
                        raise ValueError("truncated list int")
                    items.append(struct.unpack(">q", data[offset:offset + 8])[0])
                    offset += 8
            elif elem_type == TYPE_STRING:
                for _ in range(count):
                    if offset + 2 > len(data):
                        raise ValueError("truncated list string len")
                    sl = struct.unpack(">H", data[offset:offset + 2])[0]
                    offset += 2
                    if offset + sl > len(data):
                        raise ValueError("truncated list string data")
                    items.append(data[offset:offset + sl].decode("utf-8"))
                    offset += sl
            elif elem_type == TYPE_OBJECT:
                for _ in range(count):
                    obj, offset = _decode_object_v1(data, offset)
                    items.append(obj)
            else:
                raise NotImplementedError("list element type not implemented (v1)")

            value = items

        elif type_id == TYPE_OBJECT:
            obj, offset = _decode_object_v1(data, offset)
            value = obj

        else:
            raise NotImplementedError(f"type id {type_id} not implemented yet in v1")

        result[name] = value

    return result


# ---------- DECODING v2 ----------

def _decode_message_v2(data: bytes) -> dict:
    if len(data) < 6:
        raise ValueError("message too short for v2")

    version, field_count, total_len = struct.unpack(">BBI", data[:6])
    if version != 0x02:
        raise ValueError(f"v2 decoder got wrong version {version}")

    offset = 6
    result = {}

    for _ in range(field_count):
        if offset >= len(data):
            raise ValueError("truncated message (field name length) [v2]")

        name_len = data[offset]
        offset += 1

        if offset + name_len > len(data):
            raise ValueError("truncated message (field name) [v2]")

        name = data[offset:offset + name_len].decode("utf-8")
        offset += name_len

        if offset >= len(data):
            raise ValueError("truncated message (type id) [v2]")

        type_id = data[offset]
        offset += 1

        if type_id == TYPE_INT:
            if offset + 8 > len(data):
                raise ValueError("truncated int value [v2]")
            value = struct.unpack(">q", data[offset:offset + 8])[0]
            offset += 8

        elif type_id == TYPE_STRING:
            if offset + 4 > len(data):
                raise ValueError("truncated string length [v2]")
            str_len = struct.unpack(">I", data[offset:offset + 4])[0]
            offset += 4
            if offset + str_len > len(data):
                raise ValueError("truncated string data [v2]")
            value = data[offset:offset + str_len].decode("utf-8")
            offset += str_len

        elif type_id == TYPE_BYTES:
            if offset + 4 > len(data):
                raise ValueError("truncated bytes length [v2]")
            b_len = struct.unpack(">I", data[offset:offset + 4])[0]
            offset += 4
            if offset + b_len > len(data):
                raise ValueError("truncated bytes data [v2]")
            value = data[offset:offset + b_len]
            offset += b_len

        elif type_id == TYPE_LIST:
            if offset + 5 > len(data):
                raise ValueError("truncated list header [v2]")
            elem_type = data[offset]
            offset += 1
            count = struct.unpack(">I", data[offset:offset + 4])[0]
            offset += 4

            items = []
            if elem_type == TYPE_INT:
                for _ in range(count):
                    if offset + 8 > len(data):
                        raise ValueError("truncated list int [v2]")
                    items.append(struct.unpack(">q", data[offset:offset + 8])[0])
                    offset += 8

            elif elem_type == TYPE_STRING:
                for _ in range(count):
                    if offset + 4 > len(data):
                        raise ValueError("truncated list string len [v2]")
                    sl = struct.unpack(">I", data[offset:offset + 4])[0]
                    offset += 4
                    if offset + sl > len(data):
                        raise ValueError("truncated list string data [v2]")
                    items.append(data[offset:offset + sl].decode("utf-8"))
                    offset += sl

            elif elem_type == TYPE_OBJECT:
                for _ in range(count):
                    obj, offset = _decode_object_v2(data, offset)
                    items.append(obj)

            elif elem_type == TYPE_BYTES:
                for _ in range(count):
                    if offset + 4 > len(data):
                        raise ValueError("truncated list bytes len [v2]")
                    bl = struct.unpack(">I", data[offset:offset + 4])[0]
                    offset += 4
                    if offset + bl > len(data):
                        raise ValueError("truncated list bytes data [v2]")
                    items.append(data[offset:offset + bl])
                    offset += bl

            else:
                raise NotImplementedError("list element type not implemented (v2)")

            value = items

        elif type_id == TYPE_OBJECT:
            obj, offset = _decode_object_v2(data, offset)
            value = obj

        else:
            raise NotImplementedError(f"type id {type_id} not implemented yet in v2")

        result[name] = value

    return result


# ---------- PUBLIC API ----------

def decode_message(data: bytes) -> dict:
    if not data:
        raise ValueError("empty galacticbuf message")

    version = data[0]
    if version == 0x01:
        return _decode_message_v1(data)
    elif version == 0x02:
        return _decode_message_v2(data)
    else:
        raise ValueError(f"unsupported GalacticBuf version: {version}")


if __name__ == "__main__":
    # Simple self-test for v1
    msg = encode_message({
        "user_id": 1001,
        "name": "Alice",
        "scores": [100, 200, 300],
    })
    print(len(msg), msg.hex())
    print(decode_message(msg))
