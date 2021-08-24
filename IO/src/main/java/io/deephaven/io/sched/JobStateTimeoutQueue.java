/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.sched;

import io.deephaven.io.logger.Logger;

import java.util.Set;

/**
 * A priority queue (heap) for JobState instances, ordered by their deadlines. Note that this class
 * is package-private.
 */
class JobStateTimeoutQueue implements Cloneable {
    private final Logger log;

    /** the queue storage */
    private JobState[] queue;

    /** the size of the queue (invariant: size < queue.length - 1) */
    private int size = 0;

    public JobStateTimeoutQueue(Logger log, int initialSize) {
        this.log = log;
        this.queue = new JobState[initialSize];
    }

    /** clone the queue (for testing) */
    public Object clone() throws CloneNotSupportedException {
        JobStateTimeoutQueue q = (JobStateTimeoutQueue) super.clone();
        q.queue = new JobState[queue.length];
        for (int i = 1; i <= size; ++i) {
            q.queue[i] = queue[i].clone();
        }
        q.size = size;
        return q;
    }

    /** return the priority queue's size */
    int size() {
        return size;
    }

    /** Returns true if the priority queue contains no elements. */
    boolean isEmpty() {
        return size == 0;
    }

    /** Adds a job to to the timeout queue */
    void enter(JobState state, long deadline) {
        state.deadline = deadline;
        if (state.tqPos == 0) {
            if (++size == queue.length) {
                JobState[] newQueue = new JobState[2 * queue.length];
                System.arraycopy(queue, 0, newQueue, 0, size);
                queue = newQueue;
            }
            queue[size] = state;
            state.tqPos = size;
            fixUp(size);
            assert testInvariant("after fixUp in enter-add");
        } else {
            assert queue[state.tqPos] == state;
            int k = state.tqPos;
            fixDown(k);
            fixUp(k);
            assert testInvariant("after fixDown/fixUp in enter-change");
        }
    }

    /** Return the top of the timeout queue - the next timeout */
    JobState top() {
        return queue[1];
    }

    /** Remove the top element from the timeout queue. */
    void removeTop() {
        queue[1].tqPos = 0;
        if (--size == 0) {
            queue[1] = null;
        } else {
            queue[1] = queue[size + 1];
            queue[size + 1] = null; // Drop extra reference to prevent memory leak
            queue[1].tqPos = 1;
            fixDown(1);
        }
        assert testInvariant("after removeTop()");
    }

    /** remove an arbitrary element from the timeout queue */
    void remove(JobState state) {
        int k = state.tqPos;
        if (k != 0) {
            assert queue[k] == state;
            state.tqPos = 0;
            if (k == size) {
                queue[size--] = null;
            } else {
                queue[k] = queue[size];
                queue[k].tqPos = k;
                queue[size--] = null;
                fixDown(k);
                fixUp(k);
                assert testInvariant("after fixDown/fixUp in remove()");
            }
        }
        assert testInvariant("at end of remove()");
    }

    /** move queue[k] up the heap until it's deadline is >= that of its parent. */
    private void fixUp(int k) {
        if (k > 1) {
            JobState state = queue[k];
            int j = k >> 1;
            JobState parent = queue[j];
            if (parent.deadline > state.deadline) {
                queue[k] = parent;
                parent.tqPos = k;
                k = j;
                j = k >> 1;
                while (k > 1 && (parent = queue[j]).deadline > state.deadline) {
                    queue[k] = parent;
                    parent.tqPos = k;
                    k = j;
                    j = k >> 1;
                }
                queue[k] = state;
                state.tqPos = k;
            }
        }
    }

    /** move queue[k] down the heap until it's deadline is <= those of its children. */
    private void fixDown(int k) {
        int j = k << 1;
        if (j <= size) {
            JobState state = queue[k], child = queue[j], child2;
            if (j < size && (child2 = queue[j + 1]).deadline < child.deadline) {
                child = child2;
                j++;
            }
            if (child.deadline < state.deadline) {
                queue[k] = child;
                child.tqPos = k;
                k = j;
                j = k << 1;
                while (j <= size) {
                    child = queue[j];
                    if (j < size && (child2 = queue[j + 1]).deadline < child.deadline) {
                        child = child2;
                        j++;
                    }
                    if (child.deadline >= state.deadline) {
                        break;
                    }
                    queue[k] = child;
                    child.tqPos = k;
                    k = j;
                    j = k << 1;
                }
                queue[k] = state;
                state.tqPos = k;
            }
        }
    }

    boolean testInvariantAux(int i, String what) {
        if (i <= size) {
            if (queue[i].tqPos != i) {
                log.error().append(what).append(": queue[").append(i).append("].tqPos=")
                    .append(queue[i].tqPos).append(" != ").append(i).endl();
            }
            if (!testInvariantAux(i * 2, what)) {
                return false;
            }
            if (!testInvariantAux(i * 2 + 1, what)) {
                return false;
            }
            if (i > 1) {
                if (queue[i].deadline < queue[i / 2].deadline) {
                    log.error().append(what).append(": child[").append(i).append("]=")
                        .append(queue[i].deadline).append(" < parent[").append((i / 2)).append("]=")
                        .append(queue[i / 2].deadline).endl();
                    return false;
                }
            }
        }
        return true;
    }

    boolean testInvariant(String what) {
        boolean result = testInvariantAux(1, what);
        if (result) {
            for (int i = size + 1; i < queue.length; ++i) {
                if (queue[i] != null) {
                    log.error().append(what).append(": size = ").append(size).append(", child[")
                        .append(i).append("]=").append(queue[i].deadline).append(" != null").endl();
                    result = false;
                }
            }
        }
        if (result) {
            // log.info("timeoutQueue.testInvariant: OK "+what);
        }
        return result;
    }

    void junitGetAllJobs(Set<Job> jobs) {
        for (int i = 1; i <= size; ++i) {
            jobs.add(queue[i].job);
        }
    }
}
