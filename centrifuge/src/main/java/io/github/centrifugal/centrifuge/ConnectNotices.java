package io.github.centrifugal.centrifuge;

import java.util.ArrayList;
import java.util.List;


/**
 * The connect listener callbacks of one connect reply, held until the handshake has written its state, so that a
 * callback which disconnects or queues a call finds the timers armed and the queued calls written. Each callback runs
 * through {@link ClientHost#reporting}: a failure of one, an Error included, is reported after the handshake, and the
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


    void run() {
        for (Runnable task : tasks) {
            host.reporting(task).run();
        }
    }
}
