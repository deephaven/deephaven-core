#!/usr/bin/env bash
# Captures thread stacks from every JVM on the runner when test execution stalls.
#
# Gradle appends to the per-task binary result files as each test class finishes, so the newest
# mtime under any build/test-results/*/binary directory is a direct measure of test progress -- a
# wedged fork leaves it frozen. A task that is running but has not finished carries an
# in-progress-results-generic.bin, which names the stalled task in the dump.
set -u

ROOT="${WATCHDOG_ROOT:-${GITHUB_WORKSPACE:-$PWD}}"
OUT="${WATCHDOG_OUT:-$ROOT/build/test-results/watchdog}"
QUIET_SECS="${WATCHDOG_QUIET_SECS:-900}"
POLL_SECS="${WATCHDOG_POLL_SECS:-60}"
DUMP_EVERY="${WATCHDOG_DUMP_EVERY:-300}"

mkdir -p "$OUT"
echo "watchdog: root=$ROOT out=$OUT quiet=${QUIET_SECS}s dump_every=${DUMP_EVERY}s poll=${POLL_SECS}s"

# A test task that is still running carries an in-progress marker, and its binary results advance
# as each class finishes. Staleness is therefore measured per task: a task whose results have not
# advanced while other tasks keep progressing is the wedged one, and a global measure would miss it.
stalled_tasks() {
    local now newest stale dir
    now=$(date +%s)
    while IFS= read -r marker; do
        [ -n "$marker" ] || continue
        dir=$(dirname "$marker")
        newest=$(find "$dir" -name '*.bin' -printf '%T@\n' 2>/dev/null | sort -nr | head -1 | cut -d. -f1)
        [ -n "$newest" ] || continue
        stale=$(( now - newest ))
        [ "$stale" -ge "$QUIET_SECS" ] || continue
        printf '%s\t%s\n' "$stale" "${dir#"$ROOT"/}"
    done < <(find "$ROOT" -name 'in-progress-results-generic.bin' 2>/dev/null)
}

in_progress_tasks() {
    find "$ROOT" -name 'in-progress-results-generic.bin' -printf '%h\n' 2>/dev/null \
        | sed "s|^$ROOT/||; s|/build/test-results/|  ->  task: |; s|/binary$||"
}

find_jcmd() {
    local c
    for c in /opt/hostedtoolcache/Java_Temurin-Hotspot_jdk/*/x64/bin/jcmd "${JAVA_HOME:-}/bin/jcmd" "$(command -v jcmd 2>/dev/null || true)"; do
        [ -n "$c" ] && [ -x "$c" ] && echo "$c"
    done
}

last_dump=0
while true; do
    sleep "$POLL_SECS"
    stalled="$(stalled_tasks)"
    # No test task is running yet (still compiling), or every running task is progressing.
    [ -n "$stalled" ] || continue
    now=$(date +%s)
    [ $(( now - last_dump )) -ge "$DUMP_EVERY" ] || continue
    last_dump=$now

    ts=$(date -u +%Y%m%dT%H%M%SZ)
    f="$OUT/threaddump-$ts.txt"
    {
        echo "===== watchdog dump $ts ====="
        echo "----- stalled test tasks (seconds stale, task dir) -----"
        echo "$stalled"
        echo "----- all test tasks in progress -----"
        in_progress_tasks || echo "(none)"
        echo "----- uptime -----"; uptime
        echo "----- memory -----"; free -m
        echo "----- disk -----";   df -h /
        echo "----- java processes -----"
        ps -eo pid,ppid,etimes,pcpu,pmem,rss,args | grep -E "[j]ava" || echo "(none)"
    } >> "$f" 2>&1

    for pid in $(pgrep -x java 2>/dev/null); do
        echo "===== Thread.print pid=$pid =====" >> "$f"
        ok=0
        # Attaching to an unresponsive JVM can block indefinitely, which would strand the watchdog
        # on the very JVM it exists to diagnose, so each attempt is bounded.
        for jcmd in $(find_jcmd); do
            if timeout 60 "$jcmd" "$pid" Thread.print -l >> "$f" 2>/dev/null; then ok=1; break; fi
        done
        # SIGQUIT makes the JVM print its own thread dump to stdout, which gradle captures into the
        # task's binary output events, so stacks still reach the artifact when attach is unavailable.
        if [ "$ok" != 1 ]; then
            echo "(jcmd could not attach to pid $pid; sending SIGQUIT)" >> "$f"
            kill -3 "$pid" 2>/dev/null || true
        fi
    done
    echo "watchdog: wrote $f"
done
