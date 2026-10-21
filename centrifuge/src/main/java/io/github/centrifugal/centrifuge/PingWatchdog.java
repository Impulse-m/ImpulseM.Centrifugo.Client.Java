package io.github.centrifugal.centrifuge;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * The ping watchdog of a connection. The server pings an idle connection every ping interval, and every decoded
 * reply counts as a sign of life. A check runs every half window: a check which finds a reply since the previous
 * check clears the flag, and two checks in a row without one close the connection with "no ping". So a reply is
 * seen at the next check, within half a window, and the close comes between one and one and a half windows after
 * the last reply. The half window is rounded up, so for an odd window the close can come 1 ms later than one and
 * a half windows.
 * <p>
 * Only {@link #markReply} runs without the lock of this object. The rest runs under it, so a check left over from
 * an ended connection cannot schedule another check after {@link #cancel}.
 */
final class PingWatchdog {

    // Checks in a row without a reply which close the connection.
    private static final int MISSED_CHECKS_TO_CLOSE = 2;

    private final Client client;
    // Set by every decoded reply, cleared by each check. An atomic exchange, so a reply which lands during a check
    // is seen by the next one.
    private final AtomicBoolean replySeen = new AtomicBoolean();
    // Bumped when a connection ends, so a check left over from it exits without scheduling.
    private long generation = 0;
    private ScheduledFuture<?> check;


    PingWatchdog(final Client client) {
        this.client = client;
    }


    /** Half of the window in milliseconds, rounded up. */
    static long halfWindowMillis(final long pingIntervalMillis, final long maxServerPingDelayMillis) {
        return (pingIntervalMillis + maxServerPingDelayMillis + 1) / 2;
    }


    /** Called for every decoded reply, on the reader thread. */
    void markReply() {
        this.replySeen.set(true);
    }


    /** Starts the checks of the connection which has just connected. */
    synchronized void start() {
        this.scheduleCheck(this.generation, 0);
    }


    /** Ends the checks of the current connection. */
    synchronized void cancel() {
        this.generation++;
        if (this.check != null) {
            this.check.cancel(true);
            this.check = null;
        }
    }


    private synchronized void scheduleCheck(final long checkGeneration, final int idleChecks) {
        final long halfWindow = halfWindowMillis(this.client.pingInterval, this.client.getOpts().getMaxServerPingDelay());
        this.check = this.client.getScheduler().schedule(
                () -> this.client.getExecutor().submit(() -> this.runCheck(checkGeneration, idleChecks)),
                halfWindow,
                TimeUnit.MILLISECONDS
        );
    }


    // Runs on the client executor.
    private void runCheck(final long checkGeneration, final int idleChecks) {
        synchronized (this) {
            if (checkGeneration != this.generation || this.client.getState() != ClientState.CONNECTED) {
                return;
            }
            final int idle = this.replySeen.getAndSet(false) ? 0 : idleChecks + 1;
            if (idle < MISSED_CHECKS_TO_CLOSE) {
                this.scheduleCheck(checkGeneration, idle);
                return;
            }
        }
        this.client.processDisconnect(Client.CONNECTING_NO_PING, "no ping", true);
    }
}
