package io.github.centrifugal.centrifuge;

import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class LaterDelayTest {

    @Test
    public void theBoundDoublesWithEachLaterResend() {
        assertEquals(250L, RpcSession.laterDelayMillis(maximum(), 0));
        assertEquals(500L, RpcSession.laterDelayMillis(maximum(), 1));
        assertEquals(1000L, RpcSession.laterDelayMillis(maximum(), 2));
    }


    @Test
    public void theDrawRangeIsZeroToTheBoundInclusive() {
        assertEquals("the draw is over 251 values for the first resend", 251, drawBoundOf(0));
        assertEquals("the draw is over 501 values for the second resend", 501, drawBoundOf(1));
        assertEquals("the draw is over 1001 values for the third resend", 1001, drawBoundOf(2));
    }


    @Test
    public void theLowestDrawGivesNoDelay() {
        assertEquals(0L, RpcSession.laterDelayMillis(new Random() {
            @Override
            public int nextInt(int bound) {
                return 0;
            }
        }, 2));
    }


    // The cap limits the range of the draw: with 300 ms left, the third resend draws over 301 values, not 1001.
    @Test
    public void laterResendDrawRangeIsCappedAtTheRemainingDeadline() {
        final int[] drawn = {-1};
        RpcSession.laterResendDelayNanos(new Random() {
            @Override
            public int nextInt(int bound) {
                drawn[0] = bound;
                return 0;
            }
        }, 2, 300_000_000L);

        assertEquals(301, drawn[0]);
    }


    // The intermediate draw is kept as drawn when it lies inside the capped range.
    @Test
    public void laterResendIntermediateDrawIsNotChangedByTheCap() {
        long delayNanos = RpcSession.laterResendDelayNanos(new Random() {
            @Override
            public int nextInt(int bound) {
                return 150;
            }
        }, 2, 300_000_000L);

        assertEquals(150_000_000L, delayNanos);
    }


    // With draw 1 the delay is the remaining deadline, not the bound of the resend.
    @Test
    public void laterResendDelayIsCappedAtTheRemainingDeadline() {
        assertEquals(300_000_000L, RpcSession.laterResendDelayNanos(maximum(), 1, 300_000_000L));
        assertEquals(500_000_000L, RpcSession.laterResendDelayNanos(maximum(), 1, 2_000_000_000L));
    }


    // The cap is taken in whole milliseconds, rounded down: with 2.5 ms left the largest resend is 2 ms, never past the deadline.
    @Test
    public void laterResendCapRoundsTheRemainingDeadlineDownToWholeMilliseconds() {
        assertEquals(2_000_000L, RpcSession.laterResendDelayNanos(maximum(), 3, 2_500_000L));
    }


    // A random source which always returns the largest value of the range.
    private static Random maximum() {
        return new Random() {
            @Override
            public int nextInt(int bound) {
                return bound - 1;
            }
        };
    }


    // The bound of the draw of the given resend, as the random source was asked for it.
    private static int drawBoundOf(final int resends) {
        final int[] drawn = {-1};
        RpcSession.laterDelayMillis(new Random() {
            @Override
            public int nextInt(int bound) {
                drawn[0] = bound;
                return 0;
            }
        }, resends);
        return drawn[0];
    }
}
