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
    // The payload, the headers and the callback are released when the call settles: see release().
    byte[] data;
    Map<String, String> headers;
    final String affinity;
    ResultCallback<RPCResult> callback;
    // The deadline on the nano clock. It starts at the submit, so it holds across resends and the redirect fetch.
    final long deadlineNanos;
    // The timeout the call asked for, in milliseconds, checked against the connection timeout at its start.
    final long timeoutMillis;
    // The identity generation the call is bound to, -1 while it is unbound. Set when the main client confirms its identity
    // at the placement of the call, and kept across the resends of the call.
    int generation = -1;
    // The endpoint key the call goes to now: the key of the call, or the key a reroute named.
    String endpointKey;
    // Resends so far. A lost connection, a later refusal and a reroute count against the one bound.
    int resends;
    // The later refusals resent so far: the draw index of the delay of the next later resend, as the client counts them.
    int laterResends;
    // The queue of the endpoint key the call waits in, and the calls before and after it there; null while it is not queued.
    RpcPoolQueue queue;
    RpcPoolCall previous;
    RpcPoolCall next;
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
    // The redirect of the call, which is fetched when a fetch slot is free.
    RpcPool.Redirect redirect;


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
        this.callback = callback;
        this.timeoutMillis = timeoutMillis;
        this.deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    }


    // Drops what a settled call holds. The deadline task of the call keeps the call alive until the deadline on a scheduler
    // which keeps cancelled tasks (below Android API 21), so the payload must not wait there with it.
    void release() {
        data = null;
        headers = null;
        callback = null;
        redirect = null;
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
