# cdc — sparse binlog to complete committed rows

`cdc.Collector` reconstructs complete rows from a MySQL binlog v4 stream that
only carries *sparse* row images. It is a single file of Python 3 standard
library code (`cdc.py`), with no dependencies.

```python
import cdc

collector = cdc.Collector(snapshot, pk)
groups = collector.feed(chunk)     # bytes in, committed groups out
collector.finish()                 # end of stream
collector.read(table, key)         # committed row (list) or None
```

* `snapshot` — `{"schema.table": [row, ...]}`; each row is a complete row.
* `pk` — `{"schema.table": column_ordinal}` (0-based); keys are non-null
  integers.
* `feed(bytes)` — accept any chunking and return the transaction groups
  completed by the bytes seen so far.
* `finish()` — must be called at end of stream.
* `read(table, key)` — current *committed* row, or `None`.

## Output

`feed` returns a list of groups. Each group corresponds to one committed transaction and is a
list of `[table, key, old_row, new_row]` entries:

* only the **net difference per key** is reported;
* a key whose final state equals the committed state yields nothing;
* a primary-key change is reported as **delete old key → insert new key**;
* a rolled-back transaction emits no group;
* rows are always complete, with absent columns filled from the transaction's
  current value for that key;
* an explicit NULL overrides the old value.

Every commit produces exactly one group (possibly empty); groups and the
entries within them follow the order in which the transaction first touched
each key.

## Log dialect

Bare binlog v4 events: 19-byte common header, payload, 4-byte CRC32
(IEEE/zlib) trailer. No magic header, no `FORMAT_DESCRIPTION_EVENT`.

| event | code |
|---|---|
| `QUERY_EVENT` (`BEGIN` / `ROLLBACK` only) | `0x02` |
| `XID_EVENT` (commit) | `0x10` |
| `TABLE_MAP_EVENT` | `0x13` |
| `WRITE_ROWS_EVENT` v2 | `0x1e` |
| `UPDATE_ROWS_EVENT` v2 | `0x1f` |
| `DELETE_ROWS_EVENT` v2 | `0x20` |

Table maps have no optional metadata. Columns are signed 4-byte `LONG` and
UTF-8 nullable `VARCHAR`; VARCHAR values use a 1-byte length when the
declared maximum length is ≤ 255 and a 2-byte little-endian length otherwise.
Each row image's NULL bitmap is sized by the number of *present* columns,
per MySQL's `Rows_log_event` format. Table ids may be remapped at any point.

## State and failure

Committed state is one `table → key → row` map. The only other long-lived
state is the table-id map. An in-flight transaction lives in a private
overlay, is never visible through `read`, and is applied atomically on
`XID_EVENT`.

Any of the following raises `ValueError` and permanently fails the collector
(`feed` and `finish` then raise `ValueError` forever, and the pending
transaction is discarded so it cannot leak):

* bad CRC32, malformed/unknown event, bad table map, invalid bitmap,
  truncation, unsupported column type, unexpected optional metadata;
* `finish()` on a truncated stream or with an unfinished transaction.

Already committed rows stay readable after a failure.

A row event outside a transaction, `BEGIN` inside an open transaction, and an
`XID` outside one are all format errors. `feed` buffers an incomplete trailing
event instead of failing; only `finish()` rejects it.

## Tools

`tools/` holds test tooling, deliberately separate from the library:

* `binloggen.py` — minimal encoder for the dialect above.
* `replay_check.py` — an independent, naive replayer (full copy + diff) used
  as the accounting oracle.
* `verify.py` — builds a randomized workload, feeds it at every chunk
  boundary split, and asserts the collector matches the replayer's groups,
  state, and `read()` results; also runs corruption cases.
* `demo.py` — a small readable demonstration.

```sh
python3 tools/verify.py          # cross-check the collector
python3 -m unittest discover -s tests -v   # unit tests
python3 tools/demo.py            # demo
```

Both `verify.py` and the unit tests pass (see `python3 tools/verify.py`).
