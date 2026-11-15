"""Unit tests for cdc.Collector (standard library unittest only)."""

import os
import struct
import sys
import unittest
import zlib

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

import cdc                                    # noqa: E402
from binloggen import (                       # noqa: E402
    DELETE_ROWS_EVENT_V2, UPDATE_ROWS_EVENT_V2, WRITE_ROWS_EVENT_V2,
    bitmap, event, long, query, rows_event, table_map, varchar, xid,
)

ACCOUNT = [long(), varchar(24), long()]
NOTE = [long(), varchar(200)]
SNAPSHOT = {"db.account": [[1, "甲", 10], [2, "乙", 20]]}
PK = {"db.account": 0}


def account_map(table_id=1):
    return table_map(table_id, "db", "account", ACCOUNT)


def txn(*events):
    return query("BEGIN") + b"".join(events) + xid()


class WorkedExampleTest(unittest.TestCase):
    def test_sparse_update_nulls_a_column(self):
        collector = cdc.Collector(SNAPSHOT, PK)
        stream = txn(
            account_map(),
            rows_event(UPDATE_ROWS_EVENT_V2, 1, ACCOUNT,
                       bitmap([0], 3), [([0], {0: 1})],
                       bitmap([1], 3), [([1], {1: None})]),
        )
        groups = collector.feed(stream)
        self.assertEqual(groups, [[
            ["db.account", 1, [1, "甲", 10], [1, None, 10]],
        ]])
        self.assertEqual(collector.read("db.account", 1), [1, None, 10])
        self.assertEqual(collector.read("db.account", 2), [2, "乙", 20])
        collector.finish()

    def test_missing_columns_keep_current_values(self):
        collector = cdc.Collector(SNAPSHOT, PK)
        stream = txn(
            account_map(),
            # before image only has the key, after image only the balance
            rows_event(UPDATE_ROWS_EVENT_V2, 1, ACCOUNT,
                       bitmap([0], 3), [([0], {0: 2})],
                       bitmap([2], 3), [([2], {2: 99})]),
        )
        collector.feed(stream)
        self.assertEqual(collector.read("db.account", 2), [2, "乙", 99])


class WorkedExampleBytesTest(unittest.TestCase):
    """Uses the exact byte shapes from the task statement.

    Before-image bitmap 0x01, after-image bitmap 0x02, and the row body
    ``00 01000000 01``: a NULL bitmap byte plus the LONG key for the before
    image, then a NULL bitmap byte with bit 0 set for the after image.
    """

    def test_exact_bytes(self):
        kinds = [long(), varchar(20)]
        snapshot = {"d.t": [[1, "甲"]]}
        pk = {"d.t": 0}
        body = table_map(1, "d", "t", kinds)
        update = event(
            UPDATE_ROWS_EVENT_V2,
            (1).to_bytes(6, "little") + b"\x00\x00" + b"\x02\x00"
            + bytes([2]) + b"\x01" + b"\x02"
            + bytes.fromhex("000100000001"),
        )
        collector = cdc.Collector(snapshot, pk)
        groups = collector.feed(query("BEGIN") + body + update + xid())
        self.assertEqual(groups, [[["d.t", 1, [1, "甲"], [1, None]]]])
        self.assertEqual(collector.read("d.t", 1), [1, None])


class SparseSemanticsTest(unittest.TestCase):
    def test_later_row_in_same_txn_sees_earlier_sparse_update(self):
        collector = cdc.Collector(SNAPSHOT, PK)
        stream = txn(
            account_map(),
            rows_event(UPDATE_ROWS_EVENT_V2, 1, ACCOUNT,
                       bitmap([0], 3), [([0], {0: 1})],
                       bitmap([1], 3), [([1], {1: "丙"})]),
            # inserts key 1 again: sparse insert keeps the txn's name
            rows_event(WRITE_ROWS_EVENT_V2, 1, ACCOUNT,
                       bitmap([0, 2], 3),
                       [([0, 2], {0: 1, 2: 77})]),
        )
        groups = collector.feed(stream)
        self.assertEqual(groups[0][0][3], [1, "丙", 77])

    def test_delete_only_needs_key_and_missing_key_ignored(self):
        collector = cdc.Collector(SNAPSHOT, PK)
        stream = txn(
            account_map(),
            rows_event(DELETE_ROWS_EVENT_V2, 1, ACCOUNT,
                       bitmap([0], 3), [([0], {0: 1}), ([0], {0: 7})]),
        )
        groups = collector.feed(stream)
        self.assertEqual(groups, [[
            ["db.account", 1, [1, "甲", 10], None],
        ]])
        self.assertIsNone(collector.read("db.account", 1))
        self.assertEqual(collector.read("db.account", 2), [2, "乙", 20])

    def test_net_noop_and_resurrection(self):
        collector = cdc.Collector(SNAPSHOT, PK)
        stream = txn(
            account_map(),
            rows_event(DELETE_ROWS_EVENT_V2, 1, ACCOUNT,
                       bitmap([0], 3), [([0], {0: 1})]),
            rows_event(WRITE_ROWS_EVENT_V2, 1, ACCOUNT,
                       bitmap([0, 1, 2], 3),
                       [([0, 1, 2], {0: 1, 1: "甲", 2: 10})]),
        )
        groups = collector.feed(stream)
        self.assertEqual(groups, [[]])
        self.assertEqual(collector.read("db.account", 1), [1, "甲", 10])

    def test_key_change_is_delete_plus_insert(self):
        collector = cdc.Collector(SNAPSHOT, PK)
        stream = txn(
            account_map(),
            rows_event(UPDATE_ROWS_EVENT_V2, 1, ACCOUNT,
                       bitmap([0], 3), [([0], {0: 1})],
                       bitmap([0, 1], 3), [([0, 1], {0: 5, 1: "丁"})]),
        )
        groups = collector.feed(stream)
        self.assertEqual(groups, [[
            ["db.account", 1, [1, "甲", 10], None],
            ["db.account", 5, None, [5, "丁", 10]],
        ]])
        self.assertIsNone(collector.read("db.account", 1))
        self.assertEqual(collector.read("db.account", 5), [5, "丁", 10])

    def test_rollback_produces_nothing_and_leaks_nothing(self):
        collector = cdc.Collector(SNAPSHOT, PK)
        stream = (query("BEGIN") + account_map()
                  + rows_event(WRITE_ROWS_EVENT_V2, 1, ACCOUNT,
                               bitmap([0, 1], 3),
                               [([0, 1], {0: 3, 1: "鬼"})])
                  + query("ROLLBACK"))
        self.assertEqual(collector.feed(stream), [])
        self.assertIsNone(collector.read("db.account", 3))
        self.assertEqual(collector.read("db.account", 1), [1, "甲", 10])

    def test_uncommitted_state_invisible_until_xid(self):
        collector = cdc.Collector(SNAPSHOT, PK)
        collector.feed(query("BEGIN") + account_map()
                       + rows_event(WRITE_ROWS_EVENT_V2, 1, ACCOUNT,
                                    bitmap([0, 1], 3),
                                    [([0, 1], {0: 9, 1: "新"})]))
        self.assertIsNone(collector.read("db.account", 9))
        groups = collector.feed(xid())
        self.assertEqual(groups, [[
            ["db.account", 9, None, [9, "新", None]],
        ]])
        self.assertEqual(collector.read("db.account", 9), [9, "新", None])

    def test_table_id_remapping(self):
        collector = cdc.Collector(SNAPSHOT, PK)
        stream = (query("BEGIN")
                  + account_map(4)
                  + rows_event(DELETE_ROWS_EVENT_V2, 4, ACCOUNT,
                               bitmap([0], 3), [([0], {0: 1})])
                  + account_map(9)
                  + rows_event(WRITE_ROWS_EVENT_V2, 9, ACCOUNT,
                               bitmap([0, 1], 3),
                               [([0, 1], {0: 4, 1: "戊"})])
                  + xid())
        groups = collector.feed(stream)
        self.assertEqual(groups, [[
            ["db.account", 1, [1, "甲", 10], None],
            ["db.account", 4, None, [4, "戊", None]],
        ]])


class ChunkingTest(unittest.TestCase):
    def build(self):
        return (txn(account_map(),
                    rows_event(WRITE_ROWS_EVENT_V2, 1, ACCOUNT,
                               bitmap([0, 1], 3),
                               [([0, 1], {0: 5, 1: "己"})]))
                + txn(account_map(),
                      rows_event(DELETE_ROWS_EVENT_V2, 1, ACCOUNT,
                                 bitmap([0], 3), [([0], {0: 2})])))

    def test_every_single_byte_split(self):
        stream = self.build()
        for size in range(1, len(stream) + 1):
            collector = cdc.Collector(SNAPSHOT, PK)
            groups = []
            for start in range(0, len(stream), size):
                groups.extend(collector.feed(stream[start:start + size]))
            collector.finish()
            self.assertEqual(len(groups), 2, "chunk size %d" % size)
            self.assertEqual(collector.read("db.account", 5), [5, "己", None])
            self.assertIsNone(collector.read("db.account", 2))

    def test_empty_chunks_are_harmless(self):
        collector = cdc.Collector(SNAPSHOT, PK)
        self.assertEqual(collector.feed(b""), [])
        collector.finish()
        self.assertEqual(collector.read("db.account", 1), [1, "甲", 10])


class ErrorTest(unittest.TestCase):
    def assert_permanently_failed(self, stream, expect_committed=None):
        collector = cdc.Collector(SNAPSHOT, PK)
        with self.assertRaises(ValueError):
            collector.feed(stream)
        for _ in range(2):
            with self.assertRaises(ValueError):
                collector.feed(b"")
        with self.assertRaises(ValueError):
            collector.finish()
        if expect_committed is not None:
            self.assertEqual(collector.read(*expect_committed[0]),
                             expect_committed[1])
        return collector

    def test_bad_crc(self):
        good = txn(account_map())
        self.assert_permanently_failed(good[:-1] + bytes([good[-1] ^ 0xFF]))

    def test_bad_schema_terminator(self):
        body = bytearray(account_map())
        offset = 19 + 6 + 2 + 1 + len("db")
        body[offset] = 0x41
        body[-4:] = (zlib.crc32(bytes(body[:-4])) & 0xFFFFFFFF).to_bytes(
            4, "little")
        self.assert_permanently_failed(query("BEGIN") + bytes(body))

    def test_bitmap_bits_beyond_columns(self):
        body = bytearray(rows_event(WRITE_ROWS_EVENT_V2, 1, ACCOUNT,
                                    bitmap([0, 1], 3),
                                    [([0, 1], {0: 5, 1: "x"})]))
        body[19 + 8 + 2 + 1] |= 0x80
        body[-4:] = (zlib.crc32(bytes(body[:-4])) & 0xFFFFFFFF).to_bytes(
            4, "little")
        self.assert_permanently_failed(query("BEGIN") + account_map()
                                       + bytes(body))

    def test_unknown_event_type(self):
        self.assert_permanently_failed(event(0x03, b""))

    def test_table_map_with_optional_metadata(self):
        base = account_map()
        # Strip the CRC, append a TLV optional-metadata field, then re-wrap
        # the whole thing as a valid event with a fresh CRC.
        raw_body = base[19:-4] + b"\x01\x01\x00"
        bad = event(0x13, raw_body)
        self.assert_permanently_failed(query("BEGIN") + bad)

    def test_row_event_outside_transaction(self):
        self.assert_permanently_failed(account_map()
                                       + rows_event(DELETE_ROWS_EVENT_V2, 1,
                                                    ACCOUNT, bitmap([0], 3),
                                                    [([0], {0: 1})]))

    def test_unfinished_transaction_at_finish(self):
        collector = cdc.Collector(SNAPSHOT, PK)
        collector.feed(query("BEGIN") + account_map())
        with self.assertRaises(ValueError):
            collector.finish()
        with self.assertRaises(ValueError):
            collector.feed(b"")

    def test_truncated_stream_at_finish(self):
        stream = txn(account_map())
        collector = cdc.Collector(SNAPSHOT, PK)
        collector.feed(stream[:-2])
        with self.assertRaises(ValueError):
            collector.finish()
        with self.assertRaises(ValueError):
            collector.feed(b"")

    def test_failure_discards_pending_transaction_only(self):
        stream = txn(account_map(),
                     rows_event(WRITE_ROWS_EVENT_V2, 1, ACCOUNT,
                                bitmap([0, 1], 3),
                                [([0, 1], {0: 5, 1: "kept"})]))
        collector = cdc.Collector(SNAPSHOT, PK)
        collector.feed(stream)
        self.assertEqual(collector.read("db.account", 5), [5, "kept", None])
        # Now start a transaction, stage a change, and break the CRC.
        collector.feed(query("BEGIN") + account_map()
                       + rows_event(WRITE_ROWS_EVENT_V2, 1, ACCOUNT,
                                    bitmap([0, 1], 3),
                                    [([0, 1], {0: 6, 1: "ghost"})]))
        bad = query("BEGIN")[:-1] + b"\x00"
        with self.assertRaises(ValueError):
            collector.feed(bad)
        self.assertIsNone(collector.read("db.account", 6))
        self.assertEqual(collector.read("db.account", 5), [5, "kept", None])


class ConstructorTest(unittest.TestCase):
    def test_key_validation(self):
        with self.assertRaises(ValueError):
            cdc.Collector({"db.t": [[None, 1]]}, {"db.t": 0})
        with self.assertRaises(ValueError):
            cdc.Collector({"db.t": [[1, 2]]}, {"db.t": -1})
        with self.assertRaises(ValueError):
            cdc.Collector({"db.t": [[1]]}, {"other.t": 0})
        with self.assertRaises(ValueError):
            cdc.Collector({"db.t": [[1], [1]]}, {"db.t": 0})

    def test_read_rejects_unknown_input(self):
        collector = cdc.Collector(SNAPSHOT, PK)
        self.assertIsNone(collector.read("db.account", 999))
        self.assertIsNone(collector.read("nope.table", 1))
        self.assertIsNone(collector.read("db.account", None))


if __name__ == "__main__":
    unittest.main(verbosity=2)
