package io.github.centrifugal.centrifuge;

import java.util.ArrayList;
import java.util.List;


/**
 * The connect listener callbacks of one connect reply, held until the handshake has written its state, so that a
 * callback which disconnects or queues a call finds the timers armed and the queued calls written. A failure of one
 * callback, an Error included, is reported as {@link ClientHost#reportUnclassified} does after the handshake, and the
 * callbacks which follow it still run. Used on the client's executor thread.
 */
final class ConnectNotices {
    private final ClientHost host;
    private final List<Runnable> tasks = new ArrayList<>();


    ConnectNotices(ClientHost host) {
        this.host = host;
    }


    void add(Runnable task) {
        tasks.add(task);
    }


    /** Runs the callbacks in order. Returns true when one of them failed, after its failure was reported. */
    boolean run() {
        boolean failed = false;
        for (Runnable task : tasks) {
            try {
                task.run();
            } catch (Throwable failure) {
                failed = true;
                host.reportUnclassified(failure);
            }
        }
        return failed;
    }
}
