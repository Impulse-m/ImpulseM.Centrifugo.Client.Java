package io.github.centrifugal.centrifuge;

import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * The delay of a failed refresh retry: 10 s to 20 s at the first failure, doubling with each failure in a row up to
 * the cap of the options (300 s by default). The endpoints are pinned with the lowest and the highest draw; the
 * draws in between stay inside the range.
 */
public class RefreshRetryDelayTest {
    private static final int MIN_MILLIS = 10_000;
    private static final int MAX_MILLIS = AuthRefresh.RETRY_MAX_MILLIS;
    private static final int CAP_MILLIS = 300_000;
    // The lowest draw gives the minimum; the highest draw, just below one, gives the minimum plus the spread of the
    // step (10 s * 2^step), capped at the ceiling of the step.
    private static final double LOWEST_DRAW = 0.0;
    private static final double HIGHEST_DRAW = 1.0 - 1e-12;
    private static final int DRAWS_PER_STEP = 50;


    @Test
    public void retryDelayEscalatesAndStopsAtTheCap() {
        // The highest draw after each step: min(ceiling, 10 s + min(ceiling, 10 s * 2^step)).
        long[] highestDraws = {20_000L, 30_000L, 50_000L, 90_000L, 170_000L, 300_000L};

        for (int step = 0; step < highestDraws.length; step++) {
            assertEquals("the lowest draw after " + step + " failures is the minimum", MIN_MILLIS,
                    AuthRefresh.retryDelayMillis(drawsAt(LOWEST_DRAW), step, MIN_MILLIS, MAX_MILLIS, CAP_MILLIS));
            assertEquals("the highest draw after " + step + " failures is the minimum plus its spread, capped at the ceiling",
                    highestDraws[step],
                    AuthRefresh.retryDelayMillis(drawsAt(HIGHEST_DRAW), step, MIN_MILLIS, MAX_MILLIS, CAP_MILLIS));
        }
    }


    @Test
    public void retryDelayDrawsStayInsideTheRange() {
        Random random = new Random(42L);

        for (int step = 0; step < 6; step++) {
            long ceiling = AuthRefresh.retryCeilingMillis(step, MAX_MILLIS, CAP_MILLIS);
            for (int draw = 0; draw < DRAWS_PER_STEP; draw++) {
                long delay = AuthRefresh.retryDelayMillis(random, step, MIN_MILLIS, MAX_MILLIS, CAP_MILLIS);
                assertTrue("the delay after " + step + " failures is inside its range: " + delay,
                        delay >= MIN_MILLIS && delay <= ceiling);
            }
        }
    }


    @Test
    public void theCapHoldsForAStepBeyondTheDoublings() {
        assertEquals(CAP_MILLIS, AuthRefresh.retryCeilingMillis(30, MAX_MILLIS, CAP_MILLIS));
        assertEquals(CAP_MILLIS, AuthRefresh.retryDelayMillis(drawsAt(HIGHEST_DRAW), 30, MIN_MILLIS, MAX_MILLIS, CAP_MILLIS));
    }


    // The draws of the full jitter at a fixed value.
    private static Random drawsAt(final double draw) {
        return new Random() {
            @Override
            public double nextDouble() {
                return draw;
            }
        };
    }
}
