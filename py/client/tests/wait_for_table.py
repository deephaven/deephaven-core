#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import timeout_decorator
import time
import platform
from pydeephaven import Session

# To work on windows, this def needs to be on its own library,
# to be pickable via multiprocessing.
@timeout_decorator.timeout(seconds=1, use_signals=platform.system() != 'Windows')
def wait_for_table():
     session1 = Session()
     session1.run_script('t = None')

     session2 = Session()
     t = session2.empty_table(10)
     session2.bind_table('t', t)

     while 't' not in session1.tables:
          time.sleep(0.1)
          pass

