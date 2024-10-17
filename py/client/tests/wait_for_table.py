#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import timeout_decorator
import platform

# To work on windows, this def needs to be on its own library,
# to be pickable via multiprocessing.
@timeout_decorator.timeout(seconds=1, use_signals=platform.system() != 'Windows')
def wait_for_table(table_name: str, session):
     while table_name not in session.tables:
         pass

