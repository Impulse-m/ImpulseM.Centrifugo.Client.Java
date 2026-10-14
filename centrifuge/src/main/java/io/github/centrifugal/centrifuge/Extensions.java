package io.github.centrifugal.centrifuge;

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

    private Extensions() {
    }

    /** Whether the bit {@code flag} is set in the bitmask {@code flags}. */
    static boolean hasFlag(long flags, long flag) {
        return (flags & flag) != 0;
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
}
