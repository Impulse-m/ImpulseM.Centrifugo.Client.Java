package io.github.centrifugal.centrifuge;

import java.util.Map;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;
import java8.util.concurrent.CompletableFuture;


/**
 * One rpc call made with {@link Client#rpcCall}. {@link #cancel()} stops the call on the
 * server when the server supports it, and rejects the call locally with
 * {@link ReplyError#CANCELLED}.
 */
public final class RpcCall {
    private final Client client;
    final String method;
    final byte[] data;
    final Map<String, String> headers;
    final long timeout;
    final ResultCallback<RPCResult> callback;

    // The fields below are the state of the call on the client's executor thread.
    // The future is created with the call, so a cancel can complete it before the call starts.
    final CompletableFuture<Protocol.Reply> future = new CompletableFuture<>();
    long msgId;
    int commandId;
    // Written to a transport at least once.
    boolean everSent;
    // The last write carried msg_id, so the server knows the call and may answer a resend.
    boolean wireTracked;


    RpcCall(
            Client client,
            String method,
            byte[] data,
            Map<String, String> headers,
            long timeout,
            ResultCallback<RPCResult> callback
    ) {
        this.client = client;
        this.method = method;
        this.data = data;
        this.headers = headers;
        this.timeout = timeout;
        this.callback = callback;
    }


    /**
     * Cancels the call and rejects it with {@link ReplyError#CANCELLED}. A call which was not
     * sent yet is never sent. A call which was sent is cancelled on the server over the current
     * connection, or over the next one when the connection is down. On a connected server which
     * does not support RPC options, cancelling a call rejects it with {@link ReplyError#UNSUPPORTED}
     * and nothing is sent. Has no effect once the call is done.
     */
    public void cancel() {
        this.client.cancelRpc(this);
    }
}
