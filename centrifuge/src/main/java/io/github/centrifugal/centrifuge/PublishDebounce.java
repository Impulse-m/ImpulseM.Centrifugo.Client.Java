package io.github.centrifugal.centrifuge;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


/**
 * Coalesces the publishes of one subscription inside a window set by the server. The first publish
 * goes out at once and starts the window; publishes inside it replace the pending value, which is
 * sent when the window ends, and the window restarts while there is a pending value. Pending data is
 * dropped on {@link #cancel}. All methods run on the client executor.
 */
final class PublishDebounce {

    private final Client client;
    private final String channel;
    // Latest value not yet sent, null when none.
    private byte[] pending;
    private ScheduledFuture<?> timer;
    // Bumped on every arm and cancel, so a timer task which was cancelled after it had been
    // submitted to the executor does nothing.
    private long generation = 0;


    PublishDebounce(final Client client, final String channel) {
        this.client = client;
        this.channel = channel;
    }


    void publish(final byte[] data, final long intervalMillis, final ResultCallback<PublishResult> cb) {
        if (this.timer == null) {
            this.client.publish(this.channel, data, cb);
            this.arm(intervalMillis);
            return;
        }
        this.pending = data;
        cb.onDone(null, new PublishResult());
    }


    void cancel() {
        if (this.timer != null) {
            this.timer.cancel(false);
        }
        this.timer = null;
        this.pending = null;
        this.generation++;
    }


    private void arm(final long intervalMillis) {
        final long armed = ++this.generation;
        this.timer = this.client.getScheduler().schedule(
                () -> this.client.getExecutor().submit(() -> this.fire(armed, intervalMillis)),
                intervalMillis,
                TimeUnit.MILLISECONDS
        );
    }


    private void fire(final long armed, final long intervalMillis) {
        if (armed != this.generation) {
            return;
        }
        this.timer = null;
        if (this.pending == null) {
            return;
        }
        final byte[] data = this.pending;
        this.pending = null;
        this.client.publish(this.channel, data, (err, res) -> {
        });
        this.arm(intervalMillis);
    }
}
