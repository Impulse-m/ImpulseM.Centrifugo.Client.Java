package io.github.centrifugal.centrifuge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * A scheduler whose clock the test moves. A task runs when {@link #advance} moves the clock past its
 * due time, on the thread which advances it, so a test waits for no real delay. It keeps the delay of
 * every task it was given, so a test can assert the delays the client chose.
 */
final class ManualScheduler extends AbstractExecutorService implements ScheduledExecutorService {
    private final List<Task<?>> tasks = new ArrayList<>();
    private final List<Long> requestedMillis = new ArrayList<>();
    private long nowNanos = 0L;
    private boolean shutdown = false;


    /** The delays in milliseconds of the tasks scheduled so far, in order. */
    public synchronized List<Long> requestedDelays() {
        return new ArrayList<>(requestedMillis);
    }


    /** The number of tasks scheduled with the delay which neither ran nor were cancelled. */
    public synchronized int pendingWithDelay(long delayMillis) {
        int count = 0;
        for (Task<?> task : tasks) {
            if (task.delayMillis == delayMillis && task.isPending()) {
                count++;
            }
        }
        return count;
    }


    /** Moves the clock forward and runs the tasks which fall due, the earliest first. */
    public synchronized void advance(long millis) {
        long target = nowNanos + TimeUnit.MILLISECONDS.toNanos(millis);
        Task<?> next = nextDue(target);
        while (next != null) {
            nowNanos = next.dueNanos;
            next.runNow();
            next = nextDue(target);
        }
        nowNanos = target;
    }


    /** The time the clock has moved since it was created, in milliseconds. */
    public synchronized long elapsedMillis() {
        return TimeUnit.NANOSECONDS.toMillis(nowNanos);
    }


    @Override
    public synchronized ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return add(Executors.callable(command), delay, unit);
    }


    @Override
    public synchronized <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return add(callable, delay, unit);
    }


    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        throw new UnsupportedOperationException("the manual scheduler runs one-shot tasks");
    }


    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException("the manual scheduler runs one-shot tasks");
    }


    @Override
    public void execute(Runnable command) {
        command.run();
    }


    @Override
    public synchronized void shutdown() {
        shutdown = true;
    }


    @Override
    public synchronized List<Runnable> shutdownNow() {
        shutdown = true;
        for (Task<?> task : tasks) {
            task.cancel(false);
        }
        return Collections.emptyList();
    }


    @Override
    public synchronized boolean isShutdown() {
        return shutdown;
    }


    @Override
    public synchronized boolean isTerminated() {
        if (!shutdown) {
            return false;
        }
        for (Task<?> task : tasks) {
            if (task.isPending()) {
                return false;
            }
        }
        return true;
    }


    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        return isTerminated();
    }


    private synchronized <V> Task<V> add(Callable<V> callable, long delay, TimeUnit unit) {
        if (shutdown) {
            throw new RejectedExecutionException("the manual scheduler is shut down");
        }
        Task<V> task = new Task<>(callable, nowNanos + unit.toNanos(delay), unit.toMillis(delay));
        tasks.add(task);
        requestedMillis.add(unit.toMillis(delay));
        return task;
    }


    private Task<?> nextDue(long targetNanos) {
        Task<?> next = null;
        for (Task<?> task : tasks) {
            if (task.isPending() && task.dueNanos <= targetNanos && (next == null || task.dueNanos < next.dueNanos)) {
                next = task;
            }
        }
        return next;
    }


    // A task of the clock. Its failure is kept in its future, as a scheduled task keeps it.
    private final class Task<V> implements ScheduledFuture<V> {
        private final FutureTask<V> work;
        private final long dueNanos;
        private final long delayMillis;
        private boolean cancelled = false;
        private boolean ran = false;


        Task(Callable<V> callable, long dueNanos, long delayMillis) {
            this.work = new FutureTask<>(callable);
            this.dueNanos = dueNanos;
            this.delayMillis = delayMillis;
        }


        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(dueNanos - nowNanos, TimeUnit.NANOSECONDS);
        }


        @Override
        public int compareTo(Delayed other) {
            return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
        }


        @Override
        public synchronized boolean cancel(boolean mayInterruptIfRunning) {
            if (!isPending()) {
                return false;
            }
            cancelled = true;
            return work.cancel(mayInterruptIfRunning);
        }


        @Override
        public synchronized boolean isCancelled() {
            return cancelled;
        }


        @Override
        public synchronized boolean isDone() {
            return cancelled || ran;
        }


        @Override
        public V get() throws InterruptedException, ExecutionException {
            return work.get();
        }


        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return work.get(timeout, unit);
        }


        synchronized boolean isPending() {
            return !cancelled && !ran;
        }


        void runNow() {
            synchronized (ManualScheduler.this) {
                if (!isPending()) {
                    return;
                }
                ran = true;
            }
            work.run();
        }
    }
}
