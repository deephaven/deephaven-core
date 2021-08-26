/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.loggers;

import java.io.*;
import java.util.*;

public class Log4JTimedBufferedWriter extends BufferedWriter {

    private static final ArrayList<Log4JTimedBufferedWriter> writers_ = new ArrayList<Log4JTimedBufferedWriter>();

    private static Thread thread_ = null;
    private static boolean isDone_ = false;

    private static final int flushTime_ = 3000; // cannot be a property due to log4j would need configuration which
                                                // needs log4j

    public Log4JTimedBufferedWriter(Writer out) {
        super(out);

        init();
    }

    public Log4JTimedBufferedWriter(Writer out, int sz) {
        super(out, sz);

        init();
    }

    private synchronized void init() {
        synchronized (writers_) {
            writers_.add(this);
        }

        if (thread_ == null) {
            initThread();
        }
    }

    private void initThread() {
        isDone_ = false;

        thread_ = new Thread("TimedBufferedWriter") {
            public void run() {
                while (!isDone_) {
                    Log4JTimedBufferedWriter writers[];

                    synchronized (writers_) {
                        writers = writers_.toArray(new Log4JTimedBufferedWriter[writers_.size()]);
                    }

                    if (writers.length > 0) {
                        int sleepTime = flushTime_ / writers.length;

                        for (int i = 0; i < writers.length && !isDone_; i++) {
                            writers[i].flushInternal();

                            try {
                                Thread.sleep(sleepTime);
                            } catch (InterruptedException e) {
                                // shouldn't happen
                            }
                        }
                    }
                }
            }
        };

        thread_.setDaemon(true);
        thread_.start();
    }

    private void flushInternal() {
        try {
            super.flush();
        } catch (IOException e) {
            // the stream has probably been closed...
        }
    }

    public void close() {
        try {
            super.close();
        } catch (IOException e) {
            // the stream has probably already been closed.
        }

        synchronized (writers_) {
            writers_.remove(this);
        }
    }

    public static void shutdown() {
        isDone_ = true;

        if (thread_ != null) {
            thread_.interrupt();
            thread_ = null;
        }

        synchronized (writers_) {
            writers_.clear();
        }

        isDone_ = false;
    }
}
