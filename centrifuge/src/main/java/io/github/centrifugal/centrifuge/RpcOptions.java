package io.github.centrifugal.centrifuge;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;


/**
 * Per-call options of an RPC. The headers are sent with that one call only and need a
 * server which confirms RPC options, see {@link ClientCapabilities}.
 */
public final class RpcOptions {
    public static class Builder {
        private final Map<String, String> headers = new LinkedHashMap<>();

        private long timeout;


        /** Adds a header. Adding the same key again replaces its value. */
        public Builder header(String key, String value) {
            Objects.requireNonNull(key, "header key");
            Objects.requireNonNull(value, "header value");
            this.headers.put(key, value);
            return this;
        }


        /**
         * Deadline of the call in milliseconds, instead of {@link Options#getTimeout()}. The
         * deadline holds across resends after a reconnect.
         */
        public Builder timeout(long timeout) {
            if (timeout <= 0) {
                throw new IllegalArgumentException("timeout must be positive");
            }
            this.timeout = timeout;
            return this;
        }


        public RpcOptions build() {
            return new RpcOptions(this);
        }
    }


    private final Map<String, String> headers;

    private final long timeout;


    /** Deadline in milliseconds, 0 when the call uses the client timeout. */
    public long getTimeout() {
        return timeout;
    }


    /** Headers sent with the call, in the order they were added. Empty when none were added. */
    public Map<String, String> getHeaders() {
        return headers;
    }


    private RpcOptions(Builder builder) {
        this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(builder.headers));
        this.timeout = builder.timeout;
    }
}
