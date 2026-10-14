package io.github.centrifugal.centrifuge;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntUnaryOperator;


/**
 * Issues the msg_ids of the rpc calls. A msg_id is the server-corrected unix second shifted left
 * by {@link #SEQUENCE_BITS}, with a sequence in the low bits, so it fits 53 bits. The sequence of a
 * second starts at a random lane, so two generators which share an instance rarely issue the same
 * msg_id. A backward correction of the clock holds the time part, so the msg_ids stay unique within
 * the scope. {@link #resync} may lower the time part when the server refused a msg_id as outside its
 * window. Used on the client's executor thread only.
 */
final class MsgIdGenerator {
    /** Bits of the sequence part of a msg_id. */
    static final int SEQUENCE_BITS = 20;

    /** The sequence of a second moves on to the next second when it reaches this value. */
    static final long SEQUENCE_LIMIT = 1L << SEQUENCE_BITS;

    /** Largest time part a msg_id carries: 33 bits of unix seconds. */
    static final long MAX_SECONDS = (1L << 33) - 1;

    // A lane is below this bound, so a second holds at least 2^19 msg_ids from its lane on.
    private static final int LANE_BOUND = 1 << 19;

    private final IntUnaryOperator laneSource;
    // The last msg_id issued is lastSeconds and lastSeq.
    private long lastSeconds = 0L;
    private long lastSeq = 0L;
    private long lane;


    MsgIdGenerator() {
        this((int bound) -> ThreadLocalRandom.current().nextInt(bound));
    }


    // The lane source returns a value in [0, bound). It is a parameter so that the tests pick the lanes.
    MsgIdGenerator(IntUnaryOperator laneSource) {
        this.laneSource = laneSource;
        this.lane = drawLane();
    }


    /**
     * The msg_id of a call written now, for the server time in milliseconds. Greater than the msg_id
     * issued before it, unless the generator was re-synced (see {@link #resync}). The time part never
     * moves back on its own: a server time behind the last time part keeps that time part, and the
     * sequence goes on.
     *
     * @throws IllegalStateException when the time part would not fit 33 bits.
     */
    long next(long serverTimeMillis) {
        long seconds = Math.max(serverTimeMillis / 1000L, lastSeconds);
        long seq;
        if (seconds == lastSeconds) {
            seq = lastSeq + 1;
        } else {
            seq = lane;
        }
        if (seq >= SEQUENCE_LIMIT) {
            seconds++;
            seq = lane;
        }
        if (seconds > MAX_SECONDS) {
            throw new IllegalStateException("msg_id time part does not fit 33 bits");
        }
        lastSeconds = seconds;
        lastSeq = seq;
        return (seconds << SEQUENCE_BITS) | seq;
    }


    /**
     * Re-syncs the generator after the server refused a msg_id as outside its window: the time part
     * becomes the server second of {@code serverTimeMillis}, even when it is lower than the last one,
     * and the sequence starts at a new lane. The next msg_id is the first of that lane.
     * <p>
     * The msg_ids the server accepted before the re-sync may still be recorded in the seconds the time
     * part covers again. The fresh lane makes a collision with them improbable, not impossible.
     */
    void resync(long serverTimeMillis) {
        lastSeconds = serverTimeMillis / 1000L;
        lane = drawLane();
        lastSeq = lane - 1;
    }


    private long drawLane() {
        return laneSource.applyAsInt(LANE_BOUND);
    }
}
