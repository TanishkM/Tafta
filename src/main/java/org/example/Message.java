package org.example;


public class Message {
    private long offset;
    private byte[] key;
    private byte[] value;
    private long timestamp;

    public Message(long offset, byte[] key, byte[] value, long timestamp) {
        this.offset = offset;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }


    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
