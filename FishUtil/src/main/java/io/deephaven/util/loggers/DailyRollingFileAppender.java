/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.loggers;

import org.apache.log4j.*;
import org.apache.log4j.spi.*;
import org.apache.log4j.helpers.*;

import java.io.*;

public class DailyRollingFileAppender extends org.apache.log4j.DailyRollingFileAppender {

    private boolean isActivated = false;

    public DailyRollingFileAppender() {
        super();
    }

    public DailyRollingFileAppender(Layout layout, String filename, String datePattern)
        throws IOException {
        super(layout, filename, datePattern);
    }

    public void activateOptions() {
        // i'll activate it myself...
    }

    public synchronized void setFile(String fileName, boolean append, boolean bufferedIO,
        int bufferSize) throws IOException {
        LogLog.debug("setFile called: " + fileName + ", " + append);

        // It does not make sense to have immediate flush and bufferedIO.
        if (bufferedIO) {
            setImmediateFlush(false);
        }

        reset();
        FileOutputStream ostream = null;
        try {
            //
            // attempt to create file
            //
            ostream = new FileOutputStream(fileName, append);
        } catch (FileNotFoundException ex) {
            //
            // if parent directory does not exist then
            // attempt to create it and try to create file
            // see bug 9150
            //
            String parentName = new File(fileName).getParent();
            if (parentName != null) {
                File parentDir = new File(parentName);
                if (!parentDir.exists() && parentDir.mkdirs()) {
                    ostream = new FileOutputStream(fileName, append);
                } else {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }
        Writer fw = createWriter(ostream);
        if (bufferedIO) {
            fw = new Log4JTimedBufferedWriter(fw, bufferSize);
        }
        this.setQWForFiles(fw);
        this.fileName = fileName;
        this.fileAppend = append;
        this.bufferedIO = bufferedIO;
        this.bufferSize = bufferSize;
        writeHeader();
        LogLog.debug("setFile ended");
    }

    public void append(LoggingEvent event) {
        if (!isActivated) {
            super.activateOptions();
            isActivated = true;
        }

        super.append(event);
    }
}
