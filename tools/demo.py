"""End-to-end demonstration of cdc.Collector on a small hand-built log."""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)

import cdc                                     # noqa: E402
from binloggen import (                        # noqa: E402
    DELETE_ROWS_EVENT_V2, UPDATE_ROWS_EVENT_V2, WRITE_ROWS_EVENT_V2,
    bitmap, long, query, rows_event, table_map, varchar, xid,
)

KINDS = [long(), varchar(24), long()]
SNAPSHOT = {
    "shop.account": [[1, "甲", 100], [2, "乙", 200]],
    "shop.note": [[10, "hello"]],
}
PK = {"shop.account": 0, "shop.note": 0}


def build_log():
    account = table_map(11, "shop", "account", KINDS)
    note_kinds = [long(), varchar(200)]
    note = table_map(12, "shop", "note", note_kinds)
    return b"".join([
        # transaction 1: insert a row and spend from an existing one
        query("BEGIN"),
        account,
        rows_event(WRITE_ROWS_EVENT_V2, 11, KINDS, bitmap([0, 1, 2], 3),
                   [([0, 1, 2], {0: 3, 1: "丙", 2: 50})]),
        rows_event(UPDATE_ROWS_EVENT_V2, 11, KINDS,
                   bitmap([0], 3), [([0], {0: 1})],
                   bitmap([2], 3), [([2], {2: 90})]),
        xid(),
        # transaction 2: the worked example, [1, "甲"] -> [1, None]
        query("BEGIN"),
        account,
        rows_event(UPDATE_ROWS_EVENT_V2, 11, KINDS,
                   bitmap([0], 3), [([0], {0: 1})],
                   bitmap([1], 3), [([1], {1: None})]),
        xid(),
        # transaction 3: key change, delete old and insert new
        query("BEGIN"),
        note,
        rows_event(UPDATE_ROWS_EVENT_V2, 12, note_kinds,
                   bitmap([0], 2), [([0], {0: 10})],
                   bitmap([0, 1], 2), [([0, 1], {0: 20, 1: "moved"})]),
        xid(),
        # transaction 4: rolled back, no output and no visible change
        query("BEGIN"),
        account,
        rows_event(DELETE_ROWS_EVENT_V2, 11, KINDS, bitmap([0], 3),
                   [([0], {0: 2})]),
        query("ROLLBACK"),
    ])


def main():
    log = build_log()
    collector = cdc.Collector(SNAPSHOT, PK)

    print("snapshot:")
    for table, rows in sorted(SNAPSHOT.items()):
        print("  %-14s %r" % (table, rows))
    print("log: %d bytes" % len(log))

    groups = []
    # Feed in deliberately awkward chunks to show arbitrary chunking.
    for start in range(0, len(log), 37):
        groups.extend(collector.feed(log[start:start + 37]))
    collector.finish()

    print("\ncommitted groups (%d):" % len(groups))
    for number, group in enumerate(groups, 1):
        print("  group %d:" % number)
        for table, key, old, new in group:
            print("    %-14s key=%-4r old=%-18r new=%r"
                  % (table, key, old, new))

    print("\nfinal committed rows:")
    for table, rows in sorted(SNAPSHOT.items()):
        keys = sorted(set(row[PK[table]] for row in rows)
                      | {entry[1] for group in groups for entry in group
                         if entry[0] == table})
        for key in keys:
            print("  %-14s %-4r -> %r" % (table, key,
                                          collector.read(table, key)))


if __name__ == "__main__":
    main()
