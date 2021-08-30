import io.deephaven.db.tables.utils.TableTools

// Use QueryScope! Careful; this leaks into the REPL state!
size_qs = 42
hello_qs = TableTools.emptyTable(size_qs)
world_qs = TableTools.timeTable("00:00:01")