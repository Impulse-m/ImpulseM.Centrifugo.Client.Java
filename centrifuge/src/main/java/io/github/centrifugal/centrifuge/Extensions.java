package io.github.centrifugal.centrifuge;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.regex.Pattern;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import java8.util.concurrent.CompletionException;


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

    private static final String UNSUPPORTED_RPC_MESSAGE = "rpc headers are not supported by the server";

    private static final Pattern INSTANCE_PATTERN = Pattern.compile("[A-Za-z0-9_-]{22,64}");


    /** Whether the bit {@code flag} is set in the bitmask {@code flags}. */
    static boolean hasFlag(long flags, long flag) {
        return (flags & flag) != 0;
    }


    /**
     * Runs the task on the executor. Once the executor is shut down it rejects the task, and the task
     * then runs on the calling thread, so a callback which the task delivers still runs exactly once.
     */
    static void runOnExecutor(ExecutorService executor, Runnable task) {
        try {
            executor.submit(task);
        } catch (RejectedExecutionException e) {
            task.run();
        }
    }


    /**
     * The failure a dependent stage wrapped in a {@link CompletionException}, or the failure itself
     * when nothing was wrapped.
     */
    static Throwable unwrapCompletion(Throwable error) {
        if (error instanceof CompletionException && error.getCause() != null) {
            return error.getCause();
        }
        return error;
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
