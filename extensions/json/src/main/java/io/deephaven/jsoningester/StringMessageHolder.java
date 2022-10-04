package io.deephaven.jsoningester;

import io.deephaven.util.clock.MicroTimer;

/**
 * Created by rbasralian on 10/3/22
 */
public class StringMessageHolder {
    private final long sendTimeMicros;
    private final long recvTimeMicros;
    private final String msg;

    public StringMessageHolder(String msg) {
        this(MicroTimer.currentTimeMicros(), msg);
    }

    public StringMessageHolder(long timeMicros, String msg) {
        this(timeMicros, timeMicros, msg);
    }

    public StringMessageHolder(long sendTimeMicros, long recvTimeMicros, String msg) {
        this.sendTimeMicros = sendTimeMicros;
        this.recvTimeMicros = recvTimeMicros;
        this.msg = msg;
    }

    public long getSendTimeMicros() {
        return sendTimeMicros;
    }

    public long getRecvTimeMicros() {
        return recvTimeMicros;
    }

    public String getMsg() {
        return msg;
    }
}
