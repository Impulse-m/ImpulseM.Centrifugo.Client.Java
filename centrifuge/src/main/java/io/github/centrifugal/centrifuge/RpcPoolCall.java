package io.github.centrifugal.centrifuge;

import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


/**
 * One call of an {@link RpcPool} from its submit to its settlement. Its fields are the state of the
 * pool's thread, which is the only thread that reads or writes them.
 */
final class RpcPoolCall {
    final String method;
    final byte[] data;
    final Map<String, String> headers;
    final String affinity;
    final long timeoutMillis;
    final ResultCallback<RPCResult> callback;
    // The deadline on the nano clock. It starts at the submit, so it holds across resends and the redirect fetch.
    final long deadlineNanos;
    // The endpoint key the call goes to now: the key of the call, or the key a reroute named.
    String endpointKey;
    // Resends so far. A lost connection, a later refusal and a reroute count against the one bound.
    int resends;
    // The call waits in the queue for a connection with room.
    boolean queued;
    // The call has its outcome: nothing runs for it any more, and its callback has been given the outcome.
    boolean settled;
    // The connection of the attempt in progress: on the wire, or lost and waiting for the connection's next event.
    // Null when no attempt is in progress.
    RpcPoolConnection connection;
    // The endpoint of the connection of the last attempt, which a redirect URL is checked against.
    String endpointUrl;
    ScheduledFuture<?> deadlineTask;
    // The resend which waits for its delay.
    ScheduledFuture<?> resendTask;
    // The redirect fetch in flight, null when none is.
    okhttp3.Call redirectCall;
    // The redirect of the call which waits for a fetch slot.
    RpcPool.Redirect redirect;
    boolean redirectWaiting;


    RpcPoolCall(
            String method,
            byte[] data,
            String endpointKey,
            Map<String, String> headers,
            String affinity,
            long timeoutMillis,
            ResultCallback<RPCResult> callback
    ) {
        this.method = method;
        this.data = data;
        this.endpointKey = endpointKey;
        this.headers = headers;
        this.affinity = affinity;
        this.timeoutMillis = timeoutMillis;
        this.callback = callback;
        this.deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    }


    // The time left until the deadline, in nanoseconds. Zero or less when the deadline has passed.
    long remainingNanos() {
        return deadlineNanos - System.nanoTime();
    }


    // The time left until the deadline, in milliseconds, at least one while the deadline has not passed.
    long remainingMillis() {
        return Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos()));
    }
}
