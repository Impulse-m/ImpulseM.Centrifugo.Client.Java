package io.github.centrifugal.centrifuge;

public class RPCResult {
    public ReplyError getError() {
        return error;
    }

    public void setError(ReplyError error) {
        this.error = error;
    }

    private ReplyError error;

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    private byte[] data;

    /** Whether the server answered the call from the record of an earlier attempt, not by running it again. */
    public boolean isCached() {
        return cached;
    }

    public void setCached(boolean cached) {
        this.cached = cached;
    }

    private boolean cached;
}
