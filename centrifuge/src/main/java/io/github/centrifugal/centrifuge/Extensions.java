package io.github.centrifugal.centrifuge;

import java.util.regex.Pattern;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;


/**
 * Helpers for the protocol extensions of the server fork: connect flag bits and
 * the arithmetic behind them. Package-private, the public surface lives on
 * {@link Client} and its result classes.
 */
final class Extensions {
    /**
     * Connect flag bit 32: the client understands RPC options (per-call headers).
     * Bit 32 is a long shift, so it is not truncated the way an int shift would be.
     */
    static final long RPC_OPTIONS_FLAG = 1L << 32;

    /** Bits of the sequence part of a msg_id, which is unix seconds shifted left by these bits. */
    static final int MSG_ID_SEQUENCE_BITS = 20;

    /** Largest msg_id a JSON number carries exactly: 53 bits. */
    static final long MSG_ID_MAX = (1L << 53) - 1;


    private static final String UNSUPPORTED_RPC_MESSAGE = "rpc headers are not supported by the server";

    private static final Pattern INSTANCE_PATTERN = Pattern.compile("[A-Za-z0-9_-]{22,64}");


    /** Whether the bit {@code flag} is set in the bitmask {@code flags}. */
    static boolean hasFlag(long flags, long flag) {
        return (flags & flag) != 0;
    }


    /**
     * The next msg_id: the server-corrected unix second shifted left by
     * {@link #MSG_ID_SEQUENCE_BITS}, or one above the last msg_id when that is not larger, so
     * msg_ids of one client strictly increase.
     */
    static long nextMsgId(long serverTimeMillis, long lastMsgId) {
        long candidate = (serverTimeMillis / 1000L) << MSG_ID_SEQUENCE_BITS;
        return candidate > lastMsgId ? candidate : lastMsgId + 1;
    }


    /** Whether the connection instance matches the pattern the server accepts. */
    static boolean isValidInstance(String instance) {
        return INSTANCE_PATTERN.matcher(instance).matches();
    }


    /**
     * Offset of the server clock from the local clock, from the server time of a reply and
     * the local time it arrived at. Null when the reply carries no server time: the proto
     * zero value (0) means unset.
     */
    static Long serverTimeOffsetFrom(long serverTime, long localTime) {
        if (serverTime == 0) {
            return null;
        }
        return serverTime - localTime;
    }


    /** Server time estimated from a local time and the offset of the server clock. */
    static long estimateServerTime(long localTime, long offset) {
        return localTime + offset;
    }


    /**
     * The reply of a call the client did not send: the server would have answered it with
     * {@link ReplyError#UNSUPPORTED}. Completes the call's future like any other reply.
     */
    static Protocol.Reply unsupportedRpcReply(int id) {
        return Protocol.Reply.newBuilder()
                .setId(id)
                .setError(Protocol.Error.newBuilder()
                        .setCode(ReplyError.UNSUPPORTED)
                        .setMessage(UNSUPPORTED_RPC_MESSAGE))
                .build();
    }


    private Extensions() {
    }
}
