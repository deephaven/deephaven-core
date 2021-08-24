/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import org.apache.log4j.*;
import org.apache.log4j.spi.*;

import java.io.*;

public class RollingFileAppender extends FileAppender {

    public RollingFileAppender(Layout layout, String filename) throws IOException {
        fileName = filename;

        rollOver();

        fileAppend = false;
        bufferedIO = false;
        bufferSize = 8192;
        super.layout = layout;
        setFile(filename, false, false, bufferSize);
    }

    private void rollOver() {
        // backup the old files just in case...

        new File(fileName + ".5.bak").delete();

        for (int i = 5; i > 1; i--) {
            new File(fileName + "." + (i - 1) + ".bak")
                .renameTo(new File(fileName + "." + i + ".bak"));
        }

        new File(fileName).renameTo(new File(fileName + ".1.bak"));

    }

    protected void subAppend(LoggingEvent event) {
        super.subAppend(event);
    }
}
