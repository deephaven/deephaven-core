package io.deephaven.jsoningester;

/**
 * Mutable implementation of StringMessageHolder. This is not strictly necessary but was helpful for porting the unit
 * tests.
 */
class TextMessage extends StringMessageHolder {

    private String text = null;
    private long timestamp = 0L;

    public TextMessage() {
        super(null);
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public long getSendTimeMicros() {
        return timestamp;
    }

    @Override
    public long getRecvTimeMicros() {
        return timestamp;
    }

    @Override
    public String getMsg() {
        return text;
    }

    public void setSenderTimestamp(long messageTimestamp) {
        this.timestamp = messageTimestamp;
    }
}
