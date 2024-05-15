#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

"""Demo / load test for running operations from coroutines against a server"""
import asyncio
import datetime
import threading
import time

from pydeephaven import Session

NUM_THREADS = 200
RUN_TIME_SECONDS = 60*60

def main():
    with Session(host="localhost", port=10000) as session:
        threads = []
        deadline = time.time() + RUN_TIME_SECONDS
        for ti in range(NUM_THREADS):
            t = threading.Thread(target=_interact_with_server, args=(session, ti, deadline,))
            threads.append(t)

        for t in threads:
            t.start()

        for t in threads:
            t.join()

def _interact_with_server(session, ti, deadline):
    print(f'THREAD {ti} START at {datetime.datetime.now()}', flush=True)
    while time.time() < deadline:
        session.run_script(f'import deephaven; t1_{ti} = deephaven.time_table("PT0.1S")')
        time.sleep(2)
        table = session.open_table(f't1_{ti}')
        pa_table = table.to_arrow()
        time.sleep(1)
    print(f'THREAD {ti} END at {datetime.datetime.now()}', flush=True)

if __name__ == '__main__':
    main()
