"""Independent replay accounting for :mod:`cdc`.

This is deliberately a second, naive implementation of the same semantics.
It keeps a *full copy* of the committed state plus a transaction copy, applies
row images eagerly (no overlay tricks), and then diffs the two maps.  Running
it beside :class:`cdc.Collector` (via ``tools/verify.py``) checks the
collector against an implementation that shares none of its code.

Usage as a module::

    replayer = Replayer(snapshot, pk)
    expected_groups = replayer.feed(stream)   # list of groups
    replayer.finish()

The output shape matches :meth:`cdc.Collector.feed`.
"""

import struct
import zlib

__all__ = ["Replayer", "ReplayError"]


class ReplayError(ValueError):
    pass


def _lenenc(data, pos):
    first = data[pos]
    pos += 1
    if first < 0xFB:
        return first, pos
    size = {0xFC: 2, 0xFD: 3, 0xFE: 8}.get(first)
    if size is None:
        raise ReplayError("bad length-encoded integer")
    return int.from_bytes(data[pos:pos + size], "little"), pos + size


def _bits(bitmap, index):
    return bool(bitmap[index >> 3] & (1 << (index & 7)))


class Replayer(object):
    def __init__(self, snapshot, pk):
        self.pk = dict(pk)
        self.committed = {}
        for table in self.pk:
            self.committed[table] = {}
        for table, rows in snapshot.items():
            for row in rows:
                self.committed[table][row[self.pk[table]]] = list(row)
        self.maps = {}
        self.buf = b""
        self.txn = None          # table -> key -> row, or None
        self.txn_order = []
        self.failed = False

    # --------------------------------------------------------------- input

    def feed(self, chunk):
        if self.failed:
            raise ReplayError("failed")
        self.buf += bytes(chunk)
        groups = []
        try:
            while len(self.buf) >= 19:
                size = struct.unpack("<I", self.buf[9:13])[0]
                if size < 23:
                    raise ReplayError("short event")
                if len(self.buf) < size:
                    break
                raw, self.buf = self.buf[:size], self.buf[size:]
                group = self._event(raw)
                if group is not None:
                    groups.append(group)
        except Exception:
            self.failed = True
            self.buf = b""
            self.txn = None
            raise
        return groups

    def finish(self):
        if self.failed:
            raise ReplayError("failed")
        if self.buf:
            self.failed = True
            raise ReplayError("truncated")
        if self.txn is not None:
            self.failed = True
            raise ReplayError("unfinished transaction")

    # -------------------------------------------------------------- events

    def _event(self, raw):
        event_type = raw[4]
        body = raw[19:-4]
        if struct.unpack("<I", raw[-4:])[0] != zlib.crc32(raw[:-4]) & 0xFFFFFFFF:
            raise ReplayError("crc")
        if event_type == 0x13:
            self._table_map(body)
        elif event_type in (0x1E, 0x1F, 0x20):
            self._rows(event_type, body)
        elif event_type == 0x02:
            self._query(body)
        elif event_type == 0x10:
            return self._commit()
        else:
            raise ReplayError("event type %d" % event_type)
        return None

    def _table_map(self, body):
        table_id = int.from_bytes(body[0:6], "little")
        pos = 8
        length, pos = _lenenc(body, pos)
        schema = body[pos:pos + length].decode()
        pos += length + 1
        length, pos = _lenenc(body, pos)
        table = body[pos:pos + length].decode()
        pos += length + 1
        count, pos = _lenenc(body, pos)
        types = body[pos:pos + count]
        pos += count
        meta_len, pos = _lenenc(body, pos)
        meta = body[pos:pos + meta_len]
        pos += meta_len + (count + 7) // 8
        if pos != len(body):
            raise ReplayError("table map trailing bytes")
        kinds = []
        at = 0
        for column_type in types:
            if column_type == 0x03:
                kinds.append((0x03, 0))
            elif column_type == 0x0F:
                kinds.append((0x0F, int.from_bytes(meta[at:at + 2], "little")))
                at += 2
            else:
                raise ReplayError("column type")
        if at != meta_len:
            raise ReplayError("metadata mismatch")
        self.maps[table_id] = ("%s.%s" % (schema, table), kinds)

    def _query(self, body):
        status_len = int.from_bytes(body[11:13], "little")
        schema_len = body[8]
        pos = 13 + status_len + schema_len + 1
        sql = body[pos:].decode().strip("\x00 \t\r\n").upper()
        if sql == "BEGIN":
            if self.txn is not None:
                raise ReplayError("nested begin")
            self.txn = dict((table, {}) for table in self.pk)
            self.txn_order = []
        elif sql == "ROLLBACK":
            self.txn = None
            self.txn_order = []
        else:
            raise ReplayError("query %r" % sql)

    def _rows(self, event_type, body):
        if self.txn is None:
            raise ReplayError("row outside transaction")
        table_id = int.from_bytes(body[0:6], "little")
        name, kinds = self.maps[table_id]
        extra = int.from_bytes(body[8:10], "little")
        pos = 8 + extra
        count, pos = _lenenc(body, pos)
        if count != len(kinds):
            raise ReplayError("column count")
        before = body[pos:pos + (count + 7) // 8]
        pos += (count + 7) // 8
        after = before
        if event_type == 0x1F:
            after = body[pos:pos + (count + 7) // 8]
            pos += (count + 7) // 8

        def image(bitmap):
            nonlocal pos
            present = [i for i in range(count) if _bits(bitmap, i)]
            nulls = (len(present) + 7) // 8
            null_bitmap = body[pos:pos + nulls]
            pos += nulls
            values = {}
            for index, column in enumerate(present):
                if _bits(null_bitmap, index):
                    values[column] = None
                    continue
                kind, meta = kinds[column]
                if kind == 0x03:
                    values[column] = int.from_bytes(body[pos:pos + 4], "little",
                                                    signed=True)
                    pos += 4
                else:
                    width = 2 if meta > 255 else 1
                    length = int.from_bytes(body[pos:pos + width], "little")
                    pos += width
                    values[column] = body[pos:pos + length].decode()
                    pos += length
            return values

        key_column = self.pk[name]
        while pos < len(body):
            before_values = image(before)
            if event_type == 0x1F:
                after_values = image(after)
                old_key = before_values[key_column]
                new_key = after_values.get(key_column, old_key)
                prior = self._lookup(name, old_key)
                old_row = list(prior) if prior is not None else [None] * count
                for column, value in before_values.items():
                    old_row[column] = value
                new_row = list(old_row)
                for column, value in after_values.items():
                    new_row[column] = value
                if new_key == old_key:
                    self._write(name, new_key, new_row)
                else:
                    self._remove(name, old_key)
                    self._write(name, new_key, new_row)
            else:
                key = before_values[key_column]
                if event_type == 0x1E:
                    prior = self._lookup(name, key)
                    row = list(prior) if prior is not None \
                        else [None] * count
                    for column, value in before_values.items():
                        row[column] = value
                    self._write(name, key, row)
                else:
                    self._remove(name, key)
        if pos != len(body):
            raise ReplayError("row trailing bytes")

    # ------------------------------------------------------- txn primitives

    def _lookup(self, table, key):
        if key in self.txn[table]:
            return self.txn[table][key]
        return self.committed[table].get(key)

    def _write(self, table, key, row):
        if key not in self.txn[table]:
            self.txn_order.append((table, key))
        self.txn[table][key] = list(row)

    def _remove(self, table, key):
        if key not in self.txn[table]:
            self.txn_order.append((table, key))
        self.txn[table][key] = None

    def _commit(self):
        if self.txn is None:
            raise ReplayError("xid without begin")
        entries = []
        for table, key in self.txn_order:
            new = self.txn[table][key]
            old = self.committed[table].get(key)
            if new is None:
                if old is not None:
                    entries.append([table, key, list(old), None])
                    del self.committed[table][key]
            elif old is None:
                entries.append([table, key, None, list(new)])
                self.committed[table][key] = list(new)
            elif list(old) != list(new):
                entries.append([table, key, list(old), list(new)])
                self.committed[table][key] = list(new)
        self.txn = None
        self.txn_order = []
        return entries
