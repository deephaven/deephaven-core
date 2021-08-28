import io.deephaven.db.tables.utils.TableTools

// Use QueryScope! Careful; this leaks into the REPL state!
size = 42
hello = TableTools.emptyTable(size)
world = TableTools.timeTable("00:00:01")