"""Reconstruct complete, committed rows from sparse MySQL binlog v4 row events.

Public API
==========

::

    collector = cdc.Collector(snapshot, pk)
    groups = collector.feed(chunk)      # committed transaction groups
    collector.finish()                  # end of the byte stream
    row = collector.read(table, key)    # committed row (list) or None

``snapshot`` maps ``"schema.table"`` names to lists of complete rows ordered
by column ordinal.  ``pk`` maps each table name to the zero-based ordinal of
its primary-key column.  Primary keys are non-null integers.

``feed`` accepts arbitrary byte chunks and returns the list of transaction
groups completed by the bytes supplied so far.  Every commit yields exactly
one group (possibly empty), ordered by the transaction's first touch of each
key.  A group is a list of ``[table, key, old_row, new_row]`` entries; a side
that does not exist is ``None``.  Only the net per-key difference of the
transaction is reported, so a key whose final state equals its committed
state produces no entry and a rolled-back transaction produces no group at
all.  A key change appears as a delete of the old key followed by an insert
of the new key.  All rows in a report are complete rows.

Log dialect
===========

The stream is a bare sequence of binlog version 4 events: a 19-byte common
header (timestamp, type, server id, event size, log position, flags) followed
by post-header/body and a 4-byte little-endian CRC32 (IEEE polynomial,
zlib-compatible) of everything before it.  There is no magic file header and
no format description event.  Supported events:

==============================  ======  =====================================
event                           code    meaning
==============================  ======  =====================================
QUERY_EVENT                     0x02    only ``BEGIN`` and ``ROLLBACK``
XID_EVENT                       0x10    commit
TABLE_MAP_EVENT                 0x13    table id -> schema/table/columns
WRITE_ROWS_EVENT (v2)           0x1e    insert
UPDATE_ROWS_EVENT (v2)          0x1f    update
DELETE_ROWS_EVENT (v2)          0x20    delete
==============================  ======  =====================================

Table maps carry no optional metadata.  Columns are signed 4-byte ``LONG``
(``MYSQL_TYPE_LONG``, no metadata) or UTF-8 ``VARCHAR``
(``MYSQL_TYPE_VARCHAR``, two-byte little-endian maximum-length metadata);
all columns are nullable.

Row-image semantics
===================

Images are sparse.  A column absent from an image keeps the value currently
held for that key inside the transaction (falling back to the committed row);
a NULL bit in an image overwrites the old value with ``None``.  Inserts
carry complete after-images; deletes and updates carry a before-image that
contains at least the primary key.  Table ids may be remapped at any time.

State
=====

Committed state is ``_rows``: table -> key -> complete row.  The only other
long-lived state is the current table-id map (``_maps``).  An in-flight
transaction is a private overlay (``_txn``: ordered ``(table, key) -> row``);
it is never visible through ``read`` and is applied atomically
on XID, or discarded on ROLLBACK or on any failure.  ``_buf`` holds bytes of
an incomplete trailing event.

Failure
=======

Any format, CRC, or bitmap error, a truncated event at ``finish``, or an
unfinished transaction at ``finish`` raises ``ValueError``.  The collector is
then permanently failed: ``feed`` and ``finish`` raise ``ValueError`` forever
and the pending transaction overlay has been discarded.  ``read`` still
exposes only committed state and therefore never leaks the transaction.
"""

import struct
import zlib

__all__ = ["Collector"]

_QUERY_EVENT = 0x02
_XID_EVENT = 0x10
_TABLE_MAP_EVENT = 0x13
_WRITE_ROWS_EVENT_V2 = 0x1E
_UPDATE_ROWS_EVENT_V2 = 0x1F
_DELETE_ROWS_EVENT_V2 = 0x20

_ROWS_EVENTS = {
    _WRITE_ROWS_EVENT_V2: "insert",
    _UPDATE_ROWS_EVENT_V2: "update",
    _DELETE_ROWS_EVENT_V2: "delete",
}

_MYSQL_TYPE_LONG = 0x03
_MYSQL_TYPE_VARCHAR = 0x0F

_HEADER_SIZE = 19
_CRC_SIZE = 4
_MIN_EVENT_SIZE = _HEADER_SIZE + _CRC_SIZE
_ROWS_V2_POST_HEADER_SIZE = 10  # table id (6) + flags (2) + extra length (2)
_QUERY_POST_HEADER_SIZE = 13

_DELETED = object()   # key removed by the in-flight transaction
_MISSING = object()   # key never touched by the in-flight transaction
_ROLLBACK = "ROLLBACK"


def _error(message):
    raise ValueError(message)


def _need(buf, pos, count, what):
    if pos < 0 or count < 0 or pos + count > len(buf):
        _error("truncated %s" % what)


def _read_lenenc(buf, pos, what):
    """Decode a MySQL length-encoded integer at ``pos``; return (value, pos)."""
    _need(buf, pos, 1, what)
    first = buf[pos]
    pos += 1
    if first < 0xFB:
        return first, pos
    if first == 0xFC:
        size = 2
    elif first == 0xFD:
        size = 3
    elif first == 0xFE:
        size = 8
    else:  # 0xfb (NULL) and 0xff are not valid length-encoded integers
        _error("invalid length-encoded integer in %s" % what)
    _need(buf, pos, size, what)
    return int.from_bytes(buf[pos:pos + size], "little"), pos + size


def _is_key(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _popcount(value):
    return bin(value).count("1")


def _bitmap(buf, pos, ncols, what, allow_empty=False):
    """Read and validate an ``ncols``-bit columns-present bitmap.

    A bitmap with no set bits would make each row image consume zero bytes,
    so it is rejected unless ``allow_empty`` marks an after-image bitmap
    (where an empty after-image still leaves the before-image to consume).
    """
    size = (ncols + 7) // 8
    _need(buf, pos, size, "%s column bitmap" % what)
    bitmap = bytes(buf[pos:pos + size])
    if ncols % 8:
        mask = (1 << (ncols % 8)) - 1
        if bitmap[-1] & ~mask & 0xFF:
            _error("column bitmap of %s has bits beyond column %d"
                   % (what, ncols - 1))
    if not allow_empty and not any(bitmap):
        _error("column bitmap of %s selects no columns" % what)
    return bitmap, pos + size


def _has_bit(bitmap, index):
    return bool(bitmap[index >> 3] & (1 << (index & 7)))


def _is_null_bit(bitmap, index):
    return bool(bitmap[index >> 3] & (1 << (index & 7)))


def _decode_image(buf, pos, ncols, present, kinds, table):
    """Read one row image.

    ``present`` is the columns-present bitmap.  ``kinds[column]`` is
    ``(type, max_length)``.  Returns ``(values, pos)`` where ``values`` maps
    the ordinal of every *present* column to its value (``None`` when the
    NULL bit is set).  Columns absent from the image are not in the mapping,
    so callers can tell "sparse gap" from "explicit NULL".
    """
    present_count = 0
    for byte in present:
        present_count += _popcount(byte)
    null_size = (present_count + 7) // 8
    _need(buf, pos, null_size, "NULL bitmap of a %s row image" % table)
    null_bitmap = bytes(buf[pos:pos + null_size])
    pos += null_size

    values = {}
    null_index = 0
    for column in range(ncols):
        if not _has_bit(present, column):
            continue
        is_null = _is_null_bit(null_bitmap, null_index)
        null_index += 1
        if is_null:
            values[column] = None
            continue
        kind, max_length = kinds[column]
        if kind == _MYSQL_TYPE_LONG:
            _need(buf, pos, 4, "LONG value of %s" % table)
            values[column] = int.from_bytes(buf[pos:pos + 4], "little",
                                            signed=True)
            pos += 4
        elif kind == _MYSQL_TYPE_VARCHAR:
            length_size = 2 if max_length > 255 else 1
            _need(buf, pos, length_size, "VARCHAR length of %s" % table)
            length = int.from_bytes(buf[pos:pos + length_size], "little")
            pos += length_size
            _need(buf, pos, length, "VARCHAR value of %s" % table)
            raw = bytes(buf[pos:pos + length])
            try:
                values[column] = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                _error("invalid UTF-8 in VARCHAR value of %s: %s"
                       % (table, exc))
            pos += length
        else:  # pragma: no cover - table maps reject other kinds
            _error("unsupported column type in %s" % table)
    return values, pos


class _TableMap(object):
    __slots__ = ("name", "ncols", "kinds")

    def __init__(self, name, ncols, kinds):
        self.name = name
        self.ncols = ncols
        self.kinds = kinds


class Collector(object):
    """Turn a sparse binlog stream into committed, complete rows."""

    # ---------------------------------------------------------------- setup

    def __init__(self, snapshot, pk):
        if not hasattr(snapshot, "items") or not hasattr(pk, "items"):
            raise TypeError("snapshot and pk must be mappings")

        self._pk = {}
        for name, position in pk.items():
            if not isinstance(name, str) or not name:
                _error("primary-key table names must be non-empty strings")
            if not _is_key(position) or position < 0:
                _error("primary-key column for %r must be a non-negative "
                       "integer" % (name,))
            self._pk[name] = position

        self._rows = {name: {} for name in self._pk}
        for name, rows in snapshot.items():
            if not isinstance(name, str):
                _error("snapshot table names must be strings")
            if name not in self._pk:
                _error("snapshot table %r has no primary-key column" % (name,))
            try:
                rows = list(rows)
            except TypeError:
                _error("snapshot rows for %r must be a list" % (name,))
            committed = self._rows[name]
            key_position = self._pk[name]
            width = None
            for row in rows:
                if not isinstance(row, (list, tuple)):
                    _error("a snapshot row for %r is not a list" % (name,))
                if width is None:
                    width = len(row)
                elif len(row) != width:
                    _error("snapshot rows for %r have differing widths"
                           % (name,))
                if width == 0:
                    _error("snapshot table %r has no columns" % (name,))
                if key_position >= width:
                    _error("primary-key column %d is outside snapshot table "
                           "%r" % (key_position, name))
                key = row[key_position]
                if not _is_key(key):
                    _error("a snapshot row of %r has a non-integer primary "
                           "key" % (name,))
                if key in committed:
                    _error("duplicate primary key %r in snapshot table %r"
                           % (key, name))
                committed[key] = list(row)

        self._maps = {}
        self._buf = bytearray()
        # The in-flight transaction: ordered (table, key) -> row | _DELETED.
        self._txn = {}
        self._in_txn = False
        self._failed = False
        self._finished = False

    # ----------------------------------------------------------------- API

    def feed(self, chunk):
        """Consume a chunk of binlog bytes; return completed groups."""
        if self._failed:
            _error("collector has permanently failed")
        if self._finished:
            _error("collector has already finished")
        if isinstance(chunk, str) or not isinstance(
                chunk, (bytes, bytearray, memoryview)):
            raise TypeError("feed() expects a bytes-like object")

        groups = []
        self._buf.extend(bytes(chunk))
        try:
            while len(self._buf) >= _HEADER_SIZE:
                event_size = struct.unpack("<I", bytes(self._buf[9:13]))[0]
                if event_size < _MIN_EVENT_SIZE:
                    _error("event size %d is below the minimum %d"
                           % (event_size, _MIN_EVENT_SIZE))
                if len(self._buf) < event_size:
                    break
                event = bytes(self._buf[:event_size])
                del self._buf[:event_size]
                self._handle_event(event, groups)
        except ValueError:
            self._poison()
            raise
        except (struct.error, IndexError, OverflowError,
                UnicodeDecodeError) as exc:
            self._poison()
            raise ValueError("malformed binlog event: %s" % exc) from exc
        return groups

    def finish(self):
        """Mark the end of the stream; reject truncation and open transactions."""
        if self._failed:
            _error("collector has permanently failed")
        if self._finished:
            return None
        if self._buf:
            self._poison()
            _error("truncated binlog stream: %d trailing byte(s)"
                   % len(self._buf))
        if self._in_txn:
            self._poison()
            _error("unfinished transaction at end of stream")
        self._finished = True
        return None

    def read(self, table, key):
        """Return the committed row for ``(table, key)`` as a list, or None."""
        if not isinstance(table, str) or not _is_key(key):
            return None
        row = self._rows.get(table, {}).get(key)
        return None if row is None else list(row)

    # ----------------------------------------------------------- poisoning

    def _poison(self):
        self._failed = True
        self._buf = bytearray()
        self._txn = {}
        self._in_txn = False

    # ------------------------------------------------------ event dispatch

    def _handle_event(self, event, groups):
        timestamp, event_type = struct.unpack("<IB", event[0:5])
        server_id, event_size, log_pos, flags = struct.unpack(
            "<IIIH", event[5:19])
        if event_size != len(event):
            _error("event size mismatch: header says %d, got %d"
                   % (event_size, len(event)))
        payload = event[_HEADER_SIZE:-_CRC_SIZE]
        expected = struct.unpack("<I", event[-_CRC_SIZE:])[0]
        actual = zlib.crc32(event[:-_CRC_SIZE]) & 0xFFFFFFFF
        if expected != actual:
            _error("CRC32 mismatch in event type %d" % event_type)

        if event_type == _TABLE_MAP_EVENT:
            self._handle_table_map(payload)
        elif event_type in _ROWS_EVENTS:
            self._handle_rows(event_type, payload)
        elif event_type == _QUERY_EVENT:
            self._handle_query(payload)
        elif event_type == _XID_EVENT:
            if not self._in_txn:
                _error("XID event outside a transaction")
            # Every commit yields exactly one group, even when its net
            # difference is empty (compare: ROLLBACK yields no group).
            groups.append(self._commit())
        else:
            _error("unsupported binlog event type %d" % event_type)
        return groups

    # ---------------------------------------------------------- table maps

    def _handle_table_map(self, body):
        _need(body, 0, 8, "TABLE_MAP post-header")
        table_id = int.from_bytes(body[0:6], "little")
        pos = 8  # skip the 2-byte flags field

        schema_length, pos = _read_lenenc(
            body, pos, "TABLE_MAP schema-name length")
        _need(body, pos, schema_length + 1, "TABLE_MAP schema name")
        schema = body[pos:pos + schema_length]
        pos += schema_length
        if body[pos] != 0:
            _error("TABLE_MAP schema name is not NUL terminated")
        pos += 1

        name_length, pos = _read_lenenc(
            body, pos, "TABLE_MAP table-name length")
        _need(body, pos, name_length + 1, "TABLE_MAP table name")
        table = body[pos:pos + name_length]
        pos += name_length
        if body[pos] != 0:
            _error("TABLE_MAP table name is not NUL terminated")
        pos += 1

        ncols, pos = _read_lenenc(body, pos, "TABLE_MAP column count")
        if ncols <= 0:
            _error("TABLE_MAP declares no columns")
        _need(body, pos, ncols, "TABLE_MAP column types")
        column_types = bytes(body[pos:pos + ncols])
        pos += ncols

        metadata_length, pos = _read_lenenc(
            body, pos, "TABLE_MAP metadata length")
        _need(body, pos, metadata_length, "TABLE_MAP metadata")
        metadata = bytes(body[pos:pos + metadata_length])
        pos += metadata_length

        null_size = (ncols + 7) // 8
        _need(body, pos, null_size, "TABLE_MAP NULL bitmap")
        pos += null_size
        if pos != len(body):
            _error("TABLE_MAP carries unexpected trailing bytes "
                   "(optional metadata is not supported)")

        try:
            schema_name = schema.decode("utf-8")
            table_name = table.decode("utf-8")
        except UnicodeDecodeError as exc:
            _error("invalid UTF-8 in TABLE_MAP name: %s" % exc)
        if not schema_name or not table_name:
            _error("TABLE_MAP has an empty schema or table name")
        name = "%s.%s" % (schema_name, table_name)
        if name not in self._pk:
            _error("TABLE_MAP for unknown table %r" % name)

        kinds = []
        meta_pos = 0
        for column_type in column_types:
            if column_type == _MYSQL_TYPE_LONG:
                kinds.append((_MYSQL_TYPE_LONG, 0))
            elif column_type == _MYSQL_TYPE_VARCHAR:
                if meta_pos + 2 > metadata_length:
                    _error("TABLE_MAP metadata is too short for its VARCHAR "
                           "columns")
                max_length = int.from_bytes(metadata[meta_pos:meta_pos + 2],
                                            "little")
                meta_pos += 2
                if max_length <= 0:
                    _error("TABLE_MAP declares a VARCHAR with zero maximum "
                           "length")
                kinds.append((_MYSQL_TYPE_VARCHAR, max_length))
            else:
                _error("unsupported column type %d in TABLE_MAP"
                       % column_type)
        if meta_pos != metadata_length:
            _error("TABLE_MAP metadata length does not match its column types")

        # The snapshot already fixed each table's column count.  Comparing
        # against one committed row keeps widths consistent without storing
        # an extra table -> width map (an empty table adopts the log's width).
        for row in self._rows[name].values():
            if len(row) != ncols:
                _error("table %r has %d columns in the snapshot but %d in "
                       "the log" % (name, len(row), ncols))
            break
        if self._pk[name] >= ncols:
            _error("primary-key column of %r is outside its %d columns"
                   % (name, ncols))
        # A remapped table id simply takes on the newest mapping.
        self._maps[table_id] = _TableMap(name, ncols, kinds)

    # ------------------------------------------------------------ row events

    def _start_txn(self):
        self._txn = {}
        self._in_txn = True

    def _table_map_for(self, body, what):
        _need(body, 0, 6, "%s table id" % what)
        table_id = int.from_bytes(body[0:6], "little")
        table_map = self._maps.get(table_id)
        if table_map is None:
            _error("%s references unknown table id %d" % (what, table_id))
        return table_map

    def _current(self, table, key):
        """Complete row for ``key`` as the transaction currently sees it."""
        state = self._txn.get((table, key), _MISSING)
        if state is not _MISSING:
            return None if state is _DELETED else list(state)
        row = self._rows[table].get(key)
        return None if row is None else list(row)

    @staticmethod
    def _merge(base, image, ncols):
        row = [None] * ncols if base is None else list(base)
        for column, value in image.items():
            row[column] = value
        return row

    def _set_state(self, table, key, row):
        self._txn[(table, key)] = list(row)

    def _delete_state(self, table, key):
        self._txn[(table, key)] = _DELETED

    def _key_of(self, image, table, pk, where):
        if pk not in image:
            _error("%s image of %r does not contain the primary key"
                   % (where, table))
        key = image[pk]
        if not _is_key(key):
            _error("%s image of %r has a non-integer primary key"
                   % (where, table))
        return key

    def _handle_rows(self, event_type, body):
        what = _ROWS_EVENTS[event_type]
        _need(body, 0, _ROWS_V2_POST_HEADER_SIZE, "%s post-header" % what)
        table_map = self._table_map_for(body, "%s event" % what)
        pos = 8
        extra_length = int.from_bytes(body[pos:pos + 2], "little")
        pos += 2
        if extra_length == 0:
            # Tolerate writers that leave the variable header length empty;
            # it means "no extra data" rather than a negative length.
            extra_length = 2
        if extra_length < 2:
            _error("invalid extra-data length %d in %s event"
                   % (extra_length, what))
        _need(body, pos, extra_length - 2,
              "extra data of %s event" % what)
        pos += extra_length - 2

        if not self._in_txn:
            _error("%s event outside a transaction" % what)

        ncols, pos = _read_lenenc(body, pos, "%s column count" % what)
        if ncols != table_map.ncols:
            _error("%s event has %d columns but table %r has %d"
                   % (what, ncols, table_map.name, table_map.ncols))
        before_bitmap, pos = _bitmap(body, pos, ncols,
                                     "%s before-image" % what)
        if event_type == _UPDATE_ROWS_EVENT_V2:
            after_bitmap, pos = _bitmap(body, pos, ncols,
                                        "%s after-image" % what,
                                        allow_empty=True)
        else:
            after_bitmap = None

        table = table_map.name
        kinds = table_map.kinds
        pk = self._pk[table]

        if event_type == _WRITE_ROWS_EVENT_V2:
            while pos < len(body):
                image, pos = _decode_image(body, pos, ncols, before_bitmap,
                                           kinds, table)
                key = self._key_of(image, table, pk, "insert")
                row = self._merge(self._current(table, key), image, ncols)
                self._set_state(table, key, row)
        elif event_type == _DELETE_ROWS_EVENT_V2:
            while pos < len(body):
                image, pos = _decode_image(body, pos, ncols, before_bitmap,
                                           kinds, table)
                key = self._key_of(image, table, pk, "delete")
                self._delete_state(table, key)
        else:  # update
            while pos < len(body):
                before_image, pos = _decode_image(
                    body, pos, ncols, before_bitmap, kinds, table)
                old_key = self._key_of(before_image, table, pk, "before")
                after_image, pos = _decode_image(
                    body, pos, ncols, after_bitmap, kinds, table)
                if pk in after_image:
                    if not _is_key(after_image[pk]):
                        _error("update after-image of %r has a non-integer "
                               "primary key" % (table,))
                    new_key = after_image[pk]
                else:
                    new_key = old_key
                old_row = self._merge(self._current(table, old_key),
                                      before_image, ncols)
                new_row = self._merge(old_row, after_image, ncols)
                if new_key == old_key:
                    self._set_state(table, new_key, new_row)
                else:
                    # A key change is a delete of the old key plus an insert
                    # of the new key.
                    self._delete_state(table, old_key)
                    self._set_state(table, new_key, new_row)

        if pos != len(body):
            _error("%s event carries %d trailing byte(s)"
                   % (what, len(body) - pos))

    # ------------------------------------------------------------- control

    def _handle_query(self, body):
        _need(body, 0, _QUERY_POST_HEADER_SIZE, "QUERY post-header")
        schema_length = body[8]
        status_length = int.from_bytes(body[11:13], "little")
        pos = _QUERY_POST_HEADER_SIZE + status_length
        _need(body, pos, schema_length + 1, "QUERY schema name")
        pos += schema_length
        if body[pos] != 0:
            _error("QUERY schema name is not NUL terminated")
        pos += 1
        try:
            query = bytes(body[pos:]).decode("utf-8")
        except UnicodeDecodeError as exc:
            _error("invalid UTF-8 in QUERY text: %s" % exc)
        statement = query.strip("\x00 \t\r\n").upper()
        if not statement:
            _error("empty QUERY statement")
        if statement == "BEGIN":
            if self._in_txn:
                _error("BEGIN inside an open transaction")
            self._start_txn()
        elif statement == _ROLLBACK:
            self._txn = {}
            self._in_txn = False
        else:
            _error("unsupported QUERY statement %r" % query.strip())

    def _commit(self):
        """Apply the pending transaction atomically; return net-diff entries."""
        entries = []
        # ``_txn`` preserves first-touch order, so entries are reported in
        # the order in which the transaction first touched each key.
        for (table, key), state in self._txn.items():
            committed = self._rows[table]
            old = committed.get(key)
            if state is _DELETED:
                if old is not None:
                    entries.append([table, key, list(old), None])
                committed.pop(key, None)
            elif old is None:
                entries.append([table, key, None, list(state)])
                committed[key] = list(state)
            elif old != state:
                entries.append([table, key, list(old), list(state)])
                committed[key] = list(state)
            # Equal states: a net no-op, nothing to report.
        self._txn = {}
        self._in_txn = False
        return entries
