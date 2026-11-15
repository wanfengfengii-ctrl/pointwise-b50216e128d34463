"""Tiny MySQL binlog v4 encoder used by the demo and the tests.

It emits exactly the dialect :mod:`cdc` consumes: 19-byte headers, a CRC32
trailer per event, and no format-description event.  This module is test
tooling, not part of the collector.
"""

import struct
import zlib

QUERY_EVENT = 0x02
XID_EVENT = 0x10
TABLE_MAP_EVENT = 0x13
WRITE_ROWS_EVENT_V2 = 0x1E
UPDATE_ROWS_EVENT_V2 = 0x1F
DELETE_ROWS_EVENT_V2 = 0x20

LONG = 0x03
VARCHAR = 0x0F

__all__ = [
    "QUERY_EVENT", "XID_EVENT", "TABLE_MAP_EVENT",
    "WRITE_ROWS_EVENT_V2", "UPDATE_ROWS_EVENT_V2", "DELETE_ROWS_EVENT_V2",
    "LONG", "VARCHAR", "lenenc", "event", "table_map", "query", "xid",
    "bitmap", "image", "rows_event", "long", "varchar",
]


def lenenc(value):
    if value < 0xFB:
        return bytes([value])
    if value < 1 << 16:
        return b"\xfc" + value.to_bytes(2, "little")
    if value < 1 << 24:
        return b"\xfd" + value.to_bytes(3, "little")
    return b"\xfe" + value.to_bytes(8, "little")


def event(event_type, body, timestamp=1, server_id=7, log_pos=0, flags=0,
          crc=None):
    size = 19 + len(body) + 4
    header = struct.pack("<IBIIIH", timestamp, event_type, server_id, size,
                         log_pos, flags)
    raw = header + body
    checksum = zlib.crc32(raw) & 0xFFFFFFFF if crc is None else crc
    return raw + struct.pack("<I", checksum)


def query(sql, schema="", status_vars=b""):
    body = struct.pack("<IIBHH", 1, 0, len(schema), 0, len(status_vars))
    body += status_vars + schema.encode("utf-8") + b"\x00" + sql.encode("utf-8")
    return event(QUERY_EVENT, body)


def xid(value=1):
    return event(XID_EVENT, value.to_bytes(8, "little"))


def table_map(table_id, schema, table, kinds, nulls=None):
    """Build a TABLE_MAP_EVENT.

    ``kinds`` is a list of ``(type_code, metadata_bytes)`` pairs.
    """
    types = bytes(kind for kind, _ in kinds)
    metadata = b"".join(meta for _, meta in kinds)
    if nulls is None:
        nulls = bytes((len(kinds) + 7) // 8)
    body = table_id.to_bytes(6, "little") + b"\x00\x00"
    body += lenenc(len(schema.encode("utf-8"))) + schema.encode("utf-8") + b"\x00"
    body += lenenc(len(table.encode("utf-8"))) + table.encode("utf-8") + b"\x00"
    body += lenenc(len(kinds)) + types
    body += lenenc(len(metadata)) + metadata
    body += nulls
    return event(TABLE_MAP_EVENT, body)


def long():
    return (LONG, b"")


def varchar(max_length):
    return (VARCHAR, max_length.to_bytes(2, "little"))


def bitmap(present, ncols):
    out = bytearray((ncols + 7) // 8)
    for column in present:
        out[column >> 3] |= 1 << (column & 7)
    return bytes(out)


def image(kinds, present, values, ncols=None):
    """Encode one row image.

    ``present`` is an iterable of present column ordinals.  ``values`` maps
    ordinal -> value for present columns; ``None`` encodes SQL NULL.
    """
    if ncols is None:
        ncols = len(kinds)
    present_bitmap = bitmap(present, ncols)
    present_set = set(present)
    null_bytes = bytearray((len(present_set) + 7) // 8)
    payload = bytearray()
    null_index = 0
    for column in range(ncols):
        if column not in present_set:
            continue
        value = values[column]
        if value is None:
            null_bytes[null_index >> 3] |= 1 << (null_index & 7)
        else:
            kind, meta = kinds[column]
            if kind == LONG:
                payload += int(value).to_bytes(4, "little", signed=True)
            elif kind == VARCHAR:
                max_length = int.from_bytes(meta, "little")
                raw = value.encode("utf-8")
                width = 2 if max_length > 255 else 1
                payload += len(raw).to_bytes(width, "little") + raw
            else:  # pragma: no cover
                raise ValueError("unsupported kind %r" % (kind,))
        null_index += 1
    return bytes(null_bytes) + bytes(payload)


def rows_event(event_type, table_id, kinds, before_bitmap, before_rows,
               after_bitmap=None, after_rows=None, extra_data=b""):
    """Build a v2 rows event.

    ``before_rows`` / ``after_rows`` are iterables of ``(present, values)``
    pairs.  For inserts and deletes only the before image list is used.
    """
    body = table_id.to_bytes(6, "little") + b"\x00\x00"
    body += (len(extra_data) + 2).to_bytes(2, "little") + extra_data
    body += lenenc(len(kinds)) + before_bitmap
    if event_type == UPDATE_ROWS_EVENT_V2:
        body += after_bitmap
    ncols = len(kinds)
    for index, (present, values) in enumerate(before_rows):
        body += image(kinds, present, values, ncols)
        if event_type == UPDATE_ROWS_EVENT_V2:
            present_after, values_after = after_rows[index]
            body += image(kinds, present_after, values_after, ncols)
    return event(event_type, body)
