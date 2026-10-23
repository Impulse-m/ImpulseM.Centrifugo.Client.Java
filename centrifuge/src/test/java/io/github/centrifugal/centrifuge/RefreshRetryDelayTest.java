package io.github.centrifugal.centrifuge;

import io.github.centrifugal.centrifuge.internal.backoff.Backoff;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * The delay of a failed refresh retry: 10 s to 20 s at the first failure, doubling with each failure in a row up to
 * the cap of the options (300 s by default). The ceilings are exact; the delays are drawn inside their range.
 */
public class RefreshRetryDelayTest {
    private static final int MIN_MILLIS = 10_000;
    private static final int CAP_MILLIS = 300_000;
    private static final int DRAWS_PER_STEP = 50;


    @Test
    public void retryDelayEscalatesAndStopsAtTheCap() {
        int[] ceilings = {20_000, 40_000, 80_000, 160_000, 300_000, 300_000};
        Backoff backoff = new Backoff();

        for (int step = 0; step < ceilings.length; step++) {
            int ceiling = AuthRefresh.retryCeilingMillis(step, AuthRefresh.RETRY_MAX_MILLIS, CAP_MILLIS);
            assertEquals("the ceiling after " + step + " failures", ceilings[step], ceiling);
            for (int draw = 0; draw < DRAWS_PER_STEP; draw++) {
                long delay = backoff.duration(step, MIN_MILLIS, ceiling);
                assertTrue("the delay after " + step + " failures is inside its range: " + delay,
                        delay >= MIN_MILLIS && delay <= ceiling);
            }
        }
    }


    @Test
    public void theCapHoldsForAStepBeyondTheDoublings() {
        assertEquals(CAP_MILLIS, AuthRefresh.retryCeilingMillis(30, AuthRefresh.RETRY_MAX_MILLIS, CAP_MILLIS));
    }
}
