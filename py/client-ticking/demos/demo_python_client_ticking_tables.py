#
#  Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""Keeps a live client-side copy of a ticking, keyed table and reacts to changes.

The server publishes a simulated trade feed and reduces it to the latest trade per
symbol with last_by. Only five symbols are ever produced, so update cycles add rows
(new symbols) only until all five have appeared; after that, cycles modify rows
(new trades for known symbols), and a given cycle may contain only adds, only
modifies, or both. The listener:

  * applies removes, adds, and modifies to a local dict keyed by symbol
  * reads only the columns it needs, and reads the initial snapshot in chunks
  * compares modified_prev() with modified() to compute per-symbol price moves
  * shares state with the main thread under a lock
  * reports errors through on_error and stops cleanly on Ctrl+C or after a time limit

Requires Python 3.9 or later, a Deephaven server on localhost:10000, and the
pydeephaven-ticking package.

By default, this connects with anonymous authentication. A Deephaven server started
from the default configuration instead requires pre-shared key (PSK) authentication;
for such a server, set the DH_AUTH_TYPE and DH_AUTH_TOKEN environment variables, for
example:

    DH_AUTH_TYPE=io.deephaven.authentication.psk.PskAuthenticationHandler \\
    DH_AUTH_TOKEN=<your key> \\
    python demo_python_client_ticking_tables.py
"""
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass

import pydeephaven as pyd
from pydeephaven import TableListener, TableUpdate, listen

KEY = "Sym"
VALUE_COLS = ["Price", "Size"]
COLS = [KEY] + VALUE_COLS
MOVE_ALERT = 4.0  # Report price moves larger than this.
RUN_SECONDS = 30
SNAPSHOT_CHUNK = 2  # Small so that chunked reading is visible; use thousands in practice.


@dataclass
class Quote:
    price: float
    size: int
    updates: int = 1


class LiveQuoteBook(TableListener):
    """Mirrors a table keyed by KEY into a dict, and tracks price moves."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.quotes: dict[str, Quote] = {}
        self.cycles = 0
        self.error: Exception | None = None
        self.failed = threading.Event()

    def on_update(self, update: TableUpdate) -> None:
        # Apply removes before adds and modifies, so that a key that is removed and
        # re-added within one cycle ends up present.
        removed = update.removed(KEY)
        prev = update.modified_prev(COLS)
        curr = update.modified(COLS)

        alerts = []
        num_chunks = 0
        with self.lock:
            self.cycles += 1
            cycle = self.cycles

            for sym in removed.get(KEY, []):
                self.quotes.pop(sym.as_py(), None)

            # Consume the generator one chunk at a time, so that only one chunk of
            # added rows is in memory at once.
            for chunk in update.added_chunks(SNAPSHOT_CHUNK, COLS):
                num_chunks += 1
                for sym, price, size in _rows(chunk):
                    self.quotes[sym] = Quote(price, size)

            if curr:
                for (sym, old_price, _), (_, new_price, new_size) in zip(
                    _rows(prev), _rows(curr)
                ):
                    q = self.quotes.setdefault(sym, Quote(new_price, new_size, 0))
                    q.price, q.size, q.updates = new_price, new_size, q.updates + 1
                    if old_price is not None and new_price is not None:
                        move = new_price - old_price
                        if abs(move) >= MOVE_ALERT:
                            alerts.append((sym, old_price, new_price, move))

        if num_chunks > 1:
            print(f"[cycle {cycle}] read {num_chunks} chunks of adds")
        for sym, old, new, move in alerts:
            print(f"  ALERT {sym}: {old:.2f} -> {new:.2f} ({move:+.2f})")

    def on_error(self, error: Exception) -> None:
        self.error = error
        self.failed.set()

    def snapshot(self) -> tuple[int, list[tuple[str, Quote]]]:
        """Returns the cycle count and a copy of the quotes, read together under the lock."""
        with self.lock:
            quotes = sorted(
                ((s, Quote(q.price, q.size, q.updates)) for s, q in self.quotes.items()),
                key=lambda item: item[0],
            )
            return self.cycles, quotes


def _rows(cols: dict) -> list[tuple]:
    """Turns a {column: pa.Array} dict into (Sym, Price, Size) tuples."""
    if not cols:
        return []
    return list(zip(*(cols[c].to_pylist() for c in COLS)))


def make_quotes(session: pyd.Session) -> pyd.Table:
    trades = session.time_table("PT0.25s").update(
        [
            "Sym = `SYM` + randomInt(0, 5)",
            "Price = 100.0 + randomGaussian(0.0, 5.0)",
            "Size = randomInt(1, 1000)",
        ]
    )
    return trades.last_by(KEY)


def main() -> None:
    session = pyd.Session(
        host="localhost",
        port=10000,
        auth_type=os.environ.get("DH_AUTH_TYPE", "Anonymous"),
        auth_token=os.environ.get("DH_AUTH_TOKEN", ""),
    )
    book = LiveQuoteBook()
    handle = listen(make_quotes(session), book)
    handle.start()

    try:
        deadline = time.monotonic() + RUN_SECONDS
        while time.monotonic() < deadline:
            # Wake every 3 seconds, or right away if the listener fails.
            if book.failed.wait(timeout=3):
                print(f"Listener failed: {book.error}")
                break
            cycles, quotes = book.snapshot()
            print(f"--- after {cycles} cycles ---")
            for sym, q in quotes:
                print(f"{sym:5} {q.price:8.2f} {q.size:5d}  ({q.updates} updates)")
    except KeyboardInterrupt:
        print("Interrupted")
    finally:
        handle.stop()
        session.close()


if __name__ == "__main__":
    main()
