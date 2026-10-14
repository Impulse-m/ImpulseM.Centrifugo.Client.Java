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

    /**
     * Error metadata key the server sets on the errors of rpc calls it did not run. Its value says
     * whether and how the call may be sent again, see {@link #retrySignal}.
     */
    static final String RPC_RETRY_METADATA_KEY = "x-centrifugo-retry";

    /** Retry value: the msg_id was refused as outside the window. Send the call again with a new msg_id. */
    static final String RPC_RETRY_NEW_MSG_ID = "new_msg_id";

    /** Retry value: the call did not run and its msg_id is still good. Send it again, later. */
    static final String RPC_RETRY_LATER = "later";

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
     * Submits the task to the executor, and drops it once the executor is shut down: the client is
     * closed then, and the state the task would change went with it.
     */
    static void submitOrIgnore(ExecutorService executor, Runnable task) {
        try {
            executor.submit(task);
        } catch (RejectedExecutionException ignored) {
            // The client is closed.
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
     * The retry value the server set on an error reply, or null when it set none. Read through this
     * one function, so the carrier of the signal can change in one place.
     */
    static String retrySignal(Protocol.Error error) {
        return error.getMetadataMap().get(RPC_RETRY_METADATA_KEY);
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
