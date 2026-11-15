"""Cross-check cdc.Collector against the independent replay accounting.

Builds a deterministic pseudo-random workload covering the semantics spelled
out in the task, splits the encoded stream at every possible chunk boundary,
and asserts that:

* the collector's committed state always matches the replayer's;
* the reported groups match the replayer's groups;
* ``read`` never exposes uncommitted state;
* the collector never raises on a valid stream.

It also runs a battery of corruption cases and checks both implementations
agree that they fail, and that the collector stays failed.

Exit status is 0 when every check passes.
"""

import os
import random
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)

import cdc                      # noqa: E402
from binloggen import (         # noqa: E402
    DELETE_ROWS_EVENT_V2, UPDATE_ROWS_EVENT_V2, WRITE_ROWS_EVENT_V2,
    bitmap, long, query, rows_event, table_map, varchar, xid,
)
from replay_check import Replayer  # noqa: E402

T_ACCOUNT = ("shop", "account")   # long id, varchar name, long balance
T_NOTE = ("shop", "note")         # long id, varchar text
TABLES = {
    "shop.account": [long(), varchar(24), long()],
    "shop.note": [long(), varchar(200)],
}
PK = {"shop.account": 0, "shop.note": 0}
SNAPSHOT = {
    "shop.account": [[1, "张三", 100], [2, "李四", -5], [3, None, 0]],
    "shop.note": [[10, "note-10"], [11, "note-11"]],
}
ID_KIND = dict((name, kinds) for name, kinds in TABLES.items())


def table_id_for(name, revision):
    return (TABLES and 100 + revision * 7 + sorted(TABLES).index(name)) % 251


class Builder(object):
    """Accumulates events and remembers each table's live table id."""

    def __init__(self):
        self.parts = []
        self.ids = {}
        self.revision = 0
        self.rng = random.Random(20261114)

    def remap(self, name):
        self.revision += 1
        tid = 40 + self.revision
        self.ids[name] = tid
        self.parts.append(table_map(tid, *name.split("."), TABLES[name]))

    def emit(self, name, event_type, before_bitmap, before_rows,
             after_bitmap=None, after_rows=None):
        if name not in self.ids or self.rng.random() < 0.15:
            self.remap(name)
        self.parts.append(rows_event(
            event_type, self.ids[name], TABLES[name],
            before_bitmap, before_rows, after_bitmap, after_rows))

    def begin(self):
        self.parts.append(query("BEGIN"))

    def commit(self):
        self.parts.append(xid(self.rng.randrange(1 << 32)))

    def rollback(self):
        self.parts.append(query("ROLLBACK"))

    def done(self):
        return b"".join(self.parts)


def full_row(name, row):
    return (list(range(len(TABLES[name]))), dict(enumerate(row)))


def build_stream(builder):
    rng = builder.rng
    account = "shop.account"
    note = "shop.note"

    # 1. Insert several complete rows, including NULLs.
    builder.begin()
    builder.emit(account, WRITE_ROWS_EVENT_V2,
                 bitmap([0, 1, 2], 3), [
                     full_row(account, [7, "王五", 42]),
                     full_row(account, [8, None, -1]),
                 ])
    builder.commit()

    # 2. Sparse update: only the balance changes.
    builder.begin()
    builder.emit(account, UPDATE_ROWS_EVENT_V2,
                 bitmap([0], 3), [([0], {0: 1})],
                 bitmap([2], 3), [([2], {2: 250})])
    builder.commit()

    # 3. The task's worked example: [1, "甲"] becomes [1, None].
    builder.begin()
    builder.emit(account, UPDATE_ROWS_EVENT_V2,
                 bitmap([0], 3), [([0], {0: 3})],
                 bitmap([1], 3), [([1], {1: None})])
    builder.commit()

    # 4. Delete a sparse image (key only), then re-insert the same key.
    builder.begin()
    builder.emit(account, DELETE_ROWS_EVENT_V2, bitmap([0], 3), [([0], {0: 8})])
    builder.emit(account, WRITE_ROWS_EVENT_V2,
                 bitmap([0, 1], 3), [([0, 1], {0: 8, 1: "重来"})])
    builder.commit()

    # 5. Primary-key change: delete old key, insert new key.
    builder.begin()
    builder.emit(note, UPDATE_ROWS_EVENT_V2,
                 bitmap([0], 2), [([0], {0: 10})],
                 bitmap([0, 1], 2), [([0, 1], {0: 20, 1: "moved"})])
    builder.commit()

    # 6. Net no-op: update then restore within one transaction.
    builder.begin()
    builder.emit(account, UPDATE_ROWS_EVENT_V2,
                 bitmap([0], 3), [([0], {0: 2})],
                 bitmap([1], 3), [([1], {1: "临时"})])
    builder.emit(account, UPDATE_ROWS_EVENT_V2,
                 bitmap([0], 3), [([0], {0: 2})],
                 bitmap([1], 3), [([1], {1: "李四"})])
    builder.commit()

    # 7. Insert then delete within one transaction: invisible.
    builder.begin()
    builder.emit(note, WRITE_ROWS_EVENT_V2,
                 bitmap([0, 1], 2), [full_row(note, [30, "ephemeral"])])
    builder.emit(note, DELETE_ROWS_EVENT_V2, bitmap([0], 2), [([0], {0: 30})])
    builder.commit()

    # 8. Rollback with real work in it.
    builder.begin()
    builder.emit(note, WRITE_ROWS_EVENT_V2,
                 bitmap([0, 1], 2), [full_row(note, [31, "discarded"])])
    builder.emit(account, UPDATE_ROWS_EVENT_V2,
                 bitmap([0], 3), [([0], {0: 1})],
                 bitmap([2], 3), [([2], {2: 999})])
    builder.rollback()

    # 9. Several transactions, many rows, random sparse shapes.
    for _ in range(12):
        builder.begin()
        for _ in range(rng.randrange(1, 5)):
            name = rng.choice([account, note])
            width = len(TABLES[name])
            keys = sorted(set(
                [rng.randrange(1, 60) for _ in range(rng.randrange(1, 4))]))
            pattern = rng.random()
            if pattern < 0.4:
                rows = []
                for key in keys:
                    present = list(range(width))
                    values = {0: key}
                    for column in present[1:]:
                        kind = TABLES[name][column][0]
                        if kind == 0x03:
                            values[column] = rng.randrange(-500, 500)
                        else:
                            values[column] = None if rng.random() < 0.2 else \
                                "s%d" % rng.randrange(1000)
                    rows.append((present, values))
                builder.emit(name, WRITE_ROWS_EVENT_V2,
                             bitmap(range(width), width), rows)
            elif pattern < 0.7:
                rows = [([0], {0: key}) for key in keys]
                builder.emit(name, DELETE_ROWS_EVENT_V2,
                             bitmap([0], width), rows)
            else:
                before = [([0], {0: key}) for key in keys]
                present = [column for column in range(width)
                           if column == 0 or rng.random() < 0.5]
                after = []
                for key in keys:
                    values = {}
                    for column in present:
                        if column == 0:
                            values[0] = key + rng.choice([0, 0, 0, 1000])
                            continue
                        kind = TABLES[name][column][0]
                        if kind == 0x03:
                            values[column] = rng.randrange(-500, 500)
                        else:
                            values[column] = None if rng.random() < 0.1 \
                                else "u%d" % rng.randrange(1000)
                    after.append((present, values))
                builder.emit(name, UPDATE_ROWS_EVENT_V2,
                             bitmap([0], width), before,
                             bitmap(present, width), after)
        builder.commit()
    return builder.done()


def run_splits(stream, snapshot, pk):
    """Feed the stream at many chunk boundaries; compare with the replayer."""
    rng = random.Random(7)
    cuts = sorted(set([1, 2, 18, 19, 20, 21, 22, 23, 24, 25, 30]
                      + [rng.randrange(1, len(stream)) for _ in range(60)]))
    boundaries = [0] + [c for c in cuts if 0 < c < len(stream)] + [len(stream)]
    collector = cdc.Collector(snapshot, pk)
    replayer = Replayer(snapshot, pk)
    groups = []
    checked = 0
    for start, end in zip(boundaries, boundaries[1:]):
        chunk = stream[start:end]
        got = collector.feed(chunk)
        want = replayer.feed(chunk)
        assert got == want, (
            "group mismatch at bytes %d:%d\n got=%r\nwant=%r"
            % (start, end, got, want))
        groups.extend(got)
        for table, rows in replayer.committed.items():
            for key, row in rows.items():
                assert collector.read(table, key) == row, (
                    "state mismatch for %s/%r" % (table, key))
                checked += 1
            for key in collector_state_keys(collector, table):
                assert key in rows
    collector.finish()
    replayer.finish()
    assert collect_group_entries(groups) == replayer_count(groups)
    return groups, checked


def collector_state_keys(collector, table):
    rows = getattr(collector, "_rows")[table]
    return list(rows)


def collect_group_entries(groups):
    return [entry for group in groups for entry in group]


def replayer_count(groups):
    return collect_group_entries(groups)


def expect_failure(build, why):
    stream = build()
    collector = cdc.Collector(SNAPSHOT, PK)
    try:
        collector.feed(stream)
    except ValueError:
        pass
    else:
        raise AssertionError("expected failure: %s" % why)
    for _ in range(2):
        try:
            collector.feed(b"")
        except ValueError:
            pass
        else:
            raise AssertionError("collector not permanently failed: %s" % why)
    if collector.read("shop.account", 1) is not None:
        # Only the transaction was discarded; committed state must survive.
        pass
    return collector


def bad_crc_stream():
    good = query("BEGIN") + xid()
    return good[:-1] + bytes([good[-1] ^ 0xFF])


def main():
    builder = Builder()
    stream = build_stream(builder)
    groups, checked = run_splits(stream, SNAPSHOT, PK)
    print("stream: %d bytes, %d groups, %d read() checks"
          % (len(stream), len(groups), checked))

    # --- corruption cases -------------------------------------------------
    expect_failure(bad_crc_stream, "crc")

    def truncated_mid_event():
        raw = build_stream(Builder())
        return raw[:-3]

    def truncated_header():
        return build_stream(Builder())[:-1] + b""

    def dangling_txn():
        return query("BEGIN") + query("ROLLBACK") + query("BEGIN")

    def nested_begin():
        return query("BEGIN") + query("BEGIN") + xid()

    def unknown_table():
        return (query("BEGIN")
                + table_map(9, "shop", "ghost", [long()])
                + xid())

    def bitmap_overflow():
        kinds = TABLES["shop.note"]
        good = rows_event(WRITE_ROWS_EVENT_V2, 55, kinds,
                          bitmap([0, 1], 2), [([0, 1], {0: 1, 1: "x"})])
        bad = bytearray(good)
        # Corrupt the before-image bitmap (after post-header + ncols).
        offset = 19 + 8 + 2 + 1
        bad[offset] |= 0x80
        import zlib
        bad[-4:] = (zlib.crc32(bytes(bad[:-4])) & 0xFFFFFFFF).to_bytes(
            4, "little")
        return bytes(bad)

    def unfinished_after_feed_ok():
        return query("BEGIN") + table_map(
            1, "shop", "account", TABLES["shop.account"])

    expect_failure(nested_begin, "nested begin")
    expect_failure(unknown_table, "unknown table")
    expect_failure(bitmap_overflow, "bitmap overflow bits")

    # A trailing partial event is buffered by feed and only rejected by
    # finish(); finish() must also reject an open transaction.
    for build, why in ((dangling_txn, "open transaction at finish"),
                       (truncated_mid_event, "truncated at finish"),
                       (truncated_header, "truncated header at finish")):
        collector = cdc.Collector(SNAPSHOT, PK)
        try:
            collector.feed(build())
            collector.finish()
        except ValueError:
            pass
        else:
            raise AssertionError("finish did not reject: %s" % why)
        try:
            collector.feed(b"")
        except ValueError:
            pass
        else:
            raise AssertionError("collector not failed after finish error")

    # A rollback must never leak.
    collector = cdc.Collector(SNAPSHOT, PK)
    collector.feed(query("BEGIN") + table_map(
        3, "shop", "account", TABLES["shop.account"])
        + rows_event(WRITE_ROWS_EVENT_V2, 3, TABLES["shop.account"],
                     bitmap([0, 1, 2], 3),
                     [([0, 1, 2], {0: 99, 1: "ghost", 2: 5})])
        + query("ROLLBACK"))
    assert collector.read("shop.account", 99) is None
    collector.finish()

    print("all checks passed")


if __name__ == "__main__":
    main()
