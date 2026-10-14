package io.github.centrifugal.centrifuge;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ReplyError extends Throwable {
    /**
     * Client-local code of a call the client did not send, because the connection cannot carry what
     * the call asks for: headers, or a cancel, on a server which does not support RPC options, or a
     * cancel for a client without an instance. A server never sends this code.
     */
    public static final int UNSUPPORTED = 15;

    /**
     * Client-local code of a call the client did not send, because a bound is reached: the calls
     * of a client with an instance, see {@link Options#setMaxPendingRpc(int)}, or the refreshes in
     * progress, see {@link Client#refresh}. A server never sends this code.
     */
    public static final int LIMIT_EXCEEDED = 16;

    /**
     * Client-local code of a call cancelled with {@link RpcCall#cancel()}. A deadline on a sent call
     * rejects with {@link java.util.concurrent.TimeoutException}, not with this code. A server never
     * sends this code.
     */
    public static final int CANCELLED = 17;

    private int code;

    public ReplyError(int code, String message, boolean temporary) {
        this(code, message, temporary, 0, "", Collections.<String, String>emptyMap());
    }

    public ReplyError(
            int code,
            String message,
            boolean temporary,
            int appCode,
            String appType,
            Map<String, String> metadata
    ) {
        this.code = code;
        this.message = message;
        this.temporary = temporary;
        this.appCode = appCode;
        if (appType != null) {
            this.appType = appType;
        } else {
            this.appType = "";
        }
        if (metadata != null) {
            this.metadata = Collections.unmodifiableMap(new HashMap<String, String>(metadata));
        } else {
            this.metadata = Collections.emptyMap();
        }
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    private String message;

    public boolean isTemporary() {
        return temporary;
    }

    public void setTemporary(boolean temporary) {
        this.temporary = temporary;
    }

    private boolean temporary;

    /** Application error code set by the server, 0 when the server sent none. */
    public int getAppCode() {
        return appCode;
    }

    /** Application error type set by the server, empty when the server sent none. */
    public String getAppType() {
        return appType;
    }

    /** Allow-listed metadata set by the server, empty when the server sent none. */
    public Map<String, String> getMetadata() {
        return metadata;
    }

    private int appCode;
    private String appType = "";
    private Map<String, String> metadata = Collections.emptyMap();
}
