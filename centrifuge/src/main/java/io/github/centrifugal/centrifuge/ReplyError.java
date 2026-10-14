package io.github.centrifugal.centrifuge;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ReplyError extends Throwable {
    /**
     * Code of a call the client did not send because the server does not support RPC
     * options (headers) on this connection.
     */
    public static final int UNSUPPORTED = 15;

    /**
     * Code of a call the client cancelled with {@link RpcCall#cancel()}.
     */
    public static final int CANCELLED = 16;

    /**
     * Code of a call the client did not send because too many rpc calls already wait for the
     * connection, see {@link Options#setMaxPendingRpc(int)}. Same number as the server's
     * limit exceeded error.
     */
    public static final int LIMIT_EXCEEDED = 106;

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
