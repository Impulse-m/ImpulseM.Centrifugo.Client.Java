package io.github.centrifugal.centrifuge;

import java.util.HashSet;
import java.util.Set;
import java.util.function.IntUnaryOperator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


/** Unit tests of the msg_id generator. The lanes are scripted, so every msg_id is known. */
public class MsgIdGeneratorTest {
    // Largest msg_id a JSON number carries exactly: 53 bits.
    private static final long MSG_ID_MAX = (1L << 53) - 1;
    private static final long SERVER_SECOND = 1_700_000_000L;
    private static final long SERVER_MILLIS = SERVER_SECOND * 1000L;


    @Test
    public void sameSecondIncrements() {
        MsgIdGenerator generator = new MsgIdGenerator(lanes(7));

        long first = generator.next(SERVER_MILLIS + 100L);

        assertEquals((SERVER_SECOND << MsgIdGenerator.SEQUENCE_BITS) | 7L, first);
        assertEquals(first + 1, generator.next(SERVER_MILLIS + 900L));
    }


    @Test
    public void laterSecondRestartsAtTheLane() {
        MsgIdGenerator generator = new MsgIdGenerator(lanes(7));
        generator.next(SERVER_MILLIS);

        long later = generator.next(SERVER_MILLIS + 1_000L);

        assertEquals(((SERVER_SECOND + 1L) << MsgIdGenerator.SEQUENCE_BITS) | 7L, later);
    }


    @Test
    public void backwardCorrectionHoldsTheTimePartAndKeepsIncreasing() {
        MsgIdGenerator generator = new MsgIdGenerator(lanes(3));
        long ahead = generator.next(SERVER_MILLIS + 120_000L);

        long behind = generator.next(SERVER_MILLIS);
        long behindAgain = generator.next(SERVER_MILLIS);
        long caughtUp = generator.next(SERVER_MILLIS + 121_000L);

        long aheadSecond = (SERVER_SECOND + 120L);
        assertEquals(aheadSecond << MsgIdGenerator.SEQUENCE_BITS | 3L, ahead);
        assertEquals(aheadSecond << MsgIdGenerator.SEQUENCE_BITS | 4L, behind);
        assertEquals(aheadSecond << MsgIdGenerator.SEQUENCE_BITS | 5L, behindAgain);
        assertEquals((SERVER_SECOND + 121L) << MsgIdGenerator.SEQUENCE_BITS | 3L, caughtUp);
    }


    @Test
    public void overflowMovesToTheNextSecond() {
        long lane = 5L;
        MsgIdGenerator generator = new MsgIdGenerator(lanes((int) lane));
        long last = 0L;
        // The lane takes the first sequence of the second, so the sequence reaches its last value after
        // 2^20 - lane calls in all.
        for (long i = 0L; i < MsgIdGenerator.SEQUENCE_LIMIT - lane; i++) {
            last = generator.next(SERVER_MILLIS);
        }
        assertEquals((SERVER_SECOND << MsgIdGenerator.SEQUENCE_BITS) | (MsgIdGenerator.SEQUENCE_LIMIT - 1L), last);

        long overflow = generator.next(SERVER_MILLIS);

        assertEquals(((SERVER_SECOND + 1L) << MsgIdGenerator.SEQUENCE_BITS) | lane, overflow);
    }


    @Test
    public void resyncLowersTheTimePartAndChangesTheLane() {
        MsgIdGenerator generator = new MsgIdGenerator(lanes(100, 200));
        long ahead = generator.next(SERVER_MILLIS + 120_000L);
        assertEquals(((SERVER_SECOND + 120L) << MsgIdGenerator.SEQUENCE_BITS) | 100L, ahead);

        generator.resync(SERVER_MILLIS);
        long refused = generator.next(SERVER_MILLIS);

        assertEquals((SERVER_SECOND << MsgIdGenerator.SEQUENCE_BITS) | 200L, refused);
    }


    @Test
    public void twoScriptedGeneratorsInTheSameSecondGiveDifferentFirstIds() {
        MsgIdGenerator first = new MsgIdGenerator(lanes(7));
        MsgIdGenerator second = new MsgIdGenerator(lanes(9));

        assertNotEquals(first.next(SERVER_MILLIS), second.next(SERVER_MILLIS));
    }


    // The default lane source is random: the check runs with it, not with scripted lanes, so a constant
    // default lane fails here. A collision is rare: each pair has a chance below 2^-19.
    @Test
    public void twoDefaultGeneratorsInTheSameSecondGiveDifferentFirstIds() {
        for (int i = 0; i < 20; i++) {
            MsgIdGenerator first = new MsgIdGenerator();
            MsgIdGenerator second = new MsgIdGenerator();

            assertNotEquals("run " + i, first.next(SERVER_MILLIS), second.next(SERVER_MILLIS));
        }
    }


    @Test
    public void defaultLaneIsBelowTheBound() {
        for (int i = 0; i < 20; i++) {
            long sequence = new MsgIdGenerator().next(SERVER_MILLIS) & (MsgIdGenerator.SEQUENCE_LIMIT - 1);

            assertTrue("the random lane is below 2^19: " + sequence, sequence < (1L << 19));
        }
    }


    @Test
    public void msgIdsStayUniqueAndIncreasingAcrossBackwardCorrections() {
        long[] clocksMillis = {
                1_700_000_005_000L,
                1_700_000_005_000L,
                1_700_000_007_000L,
                1_700_000_003_000L,
                1_700_000_003_000L,
                1_700_000_005_000L,
                1_700_000_005_000L,
                1_700_000_008_000L,
                1_700_000_001_000L,
                1_700_000_008_000L
        };
        MsgIdGenerator generator = new MsgIdGenerator(lanes(42));
        Set<Long> issued = new HashSet<>();
        long last = 0L;
        for (long clock : clocksMillis) {
            long next = generator.next(clock);

            assertTrue(next > last);
            assertTrue("msg_id issued twice: " + next, issued.add(next));
            last = next;
        }
    }


    @Test
    public void largestTimePartFitsFiftyThreeBits() {
        MsgIdGenerator generator = new MsgIdGenerator(lanes(1));

        long msgId = generator.next(MsgIdGenerator.MAX_SECONDS * 1000L);

        assertEquals((MsgIdGenerator.MAX_SECONDS << MsgIdGenerator.SEQUENCE_BITS) | 1L, msgId);
        assertTrue(msgId <= MSG_ID_MAX);
    }


    @Test(expected = IllegalStateException.class)
    public void timePartBeyondThirtyThreeBitsIsRejected() {
        MsgIdGenerator generator = new MsgIdGenerator(lanes(1));

        generator.next((MsgIdGenerator.MAX_SECONDS + 1L) * 1000L);
    }


    // A lane source which returns the values in order, one per draw.
    private static IntUnaryOperator lanes(final int... values) {
        final int[] index = {0};
        return (int bound) -> values[index[0]++];
    }
}
