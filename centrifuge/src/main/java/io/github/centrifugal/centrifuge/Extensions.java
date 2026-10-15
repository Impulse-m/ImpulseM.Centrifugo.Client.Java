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
     * Submits the task to the executor, and drops it once the executor is shut down: the client is
     * closed then, and the state the task would change went with it.
     */
    static void submitOrIgnore(ExecutorService executor, Runnable task) {
        submitOrRun(executor, task, () -> {
        });
    }


    /**
     * Submits the task to the executor. Once the executor is shut down, which is after the client closed,
     * the task cannot run there, so {@code onRejected} runs on the calling thread instead.
     */
    static void submitOrRun(ExecutorService executor, Runnable task, Runnable onRejected) {
        try {
            executor.submit(task);
        } catch (RejectedExecutionException e) {
            onRejected.run();
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
     * The retry value the server set on an error reply, or null when it set none. Read through this
     * one function, so the carrier of the signal can change in one place.
     */
    static String retrySignal(Protocol.Error error) {
        return error.getMetadataMap().get(RPC_RETRY_METADATA_KEY);
    }


    /**
     * The reply of a call the client did not send, with no command id: the call's own future takes it.
     * The server would have answered it with {@link ReplyError#UNSUPPORTED}.
     */
    static Protocol.Reply unsupportedRpcReply() {
        return Protocol.Reply.newBuilder()
                .setError(Protocol.Error.newBuilder()
                        .setCode(ReplyError.UNSUPPORTED)
                        .setMessage(UNSUPPORTED_RPC_MESSAGE))
                .build();
    }


    /**
     * Reports a failure of the protocol handling to the listener as an error event, wrapped in an
     * {@link UnclassifiedError}. A listener which throws on the report cannot end the handling which
     * reports, so its failure is printed here.
     */
    static void reportUnclassified(EventListener listener, Client client, Throwable cause) {
        try {
            listener.onError(client, new ErrorEvent(new UnclassifiedError(cause)));
        } catch (Throwable failure) {
            failure.printStackTrace();
        }
    }


    private Extensions() {
    }
}
