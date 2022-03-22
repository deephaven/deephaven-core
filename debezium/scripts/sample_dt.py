from deephaven import PythonListenerAdapter
import datetime as dt

now_str = dt.datetime.now().astimezone().strftime('%Y%m%d%H%M%S_%Z')

log = open(f'/logs/{now_str}_dh_sample_dt.log', 'w')

def onUpdate(added_unused, modified_unused, deleted_unused):
    timestamp = dt.datetime.now().astimezone().isoformat()
    total = pageviews_summary.getColumn('total').get(0)
    max_received_at = pageviews_summary.getColumn('max_received_at').get(0)
    dt_ms = pageviews_summary.getColumn('dt_ms').get(0)
    log.write(f'timestamp={timestamp}, total={total}, max_received_at={max_received_at}, dt_ms={dt_ms}\n')
    log.flush()

PythonListenerAdapter(pageviews_summary, onUpdate, replayInitialImage=False)
