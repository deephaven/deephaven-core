import datetime as dt
import jpy
import os
import sys

def onUpdate(added_unused, modified_unused, deleted_unused):
    timestamp = dt.datetime.now().astimezone().isoformat()
    total = pageviews_summary.j_table.getColumn('total').get(0)
    max_received_at = pageviews_summary.j_table.getColumn('max_received_at').get(0)
    dt_ms = pageviews_summary.j_table.getColumn('dt_ms').get(0)
    log.write(f'timestamp={timestamp}, total={total}, max_received_at={max_received_at}, dt_ms={dt_ms}\n')
    log.flush()

now_str = dt.datetime.utcnow().astimezone().strftime('%Y.%m.%d.%H.%M.%S_%Z')

perf_tag = os.environ.get('PERF_TAG', None)
plistener = jpy.get_type('io.deephaven.integrations.python.PythonShiftObliviousListenerAdapter')
if perf_tag is not None:
    log = open(f'/logs/{perf_tag}/{now_str}_dh_sample_dt.log', 'w')
    pageviews_summary.j_table.listenForUpdates(plistener(None, pageviews_summary.j_table, True, onUpdate), False)

