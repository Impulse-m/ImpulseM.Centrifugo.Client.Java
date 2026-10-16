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
