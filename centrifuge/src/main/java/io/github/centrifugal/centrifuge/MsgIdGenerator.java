package io.github.centrifugal.centrifuge;

import java.util.Random;


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

    // Draws the lanes. It is a parameter so that the tests pick the lanes.
    private final Random random;
    // The last msg_id issued is lastSeconds and lastSeq.
    private long lastSeconds = 0L;
    private long lastSeq = 0L;
    private long lane;


    MsgIdGenerator() {
        this(new Random());
    }


    MsgIdGenerator(Random random) {
        this.random = random;
        this.lane = drawLane();
    }


    /**
     * The msg_id of a call written now, for the server time in milliseconds. Greater than the msg_id
     * issued before it, unless the generator was re-synced (see {@link #resync}). The time part never
     * moves back on its own: a server time behind the last time part keeps that time part, and the
     * sequence goes on. A time part above {@link #MAX_SECONDS} is clamped to it.
     *
     * @throws IllegalStateException when the largest second has issued every sequence value (the
     *                               msg_id space is exhausted).
     */
    long next(long serverTimeMillis) {
        long requested = Math.min(serverTimeMillis / 1000L, MAX_SECONDS);
        long seconds = Math.max(requested, lastSeconds);
        long seq;
        if (seconds == lastSeconds) {
            seq = lastSeq + 1;
        } else {
            seq = lane;
        }
        if (seq >= SEQUENCE_LIMIT) {
            if (seconds == MAX_SECONDS) {
                throw new IllegalStateException("msg_id space exhausted");
            }
            seconds++;
            seq = lane;
        }
        lastSeconds = seconds;
        lastSeq = seq;
        return (seconds << SEQUENCE_BITS) | seq;
    }


    /**
     * Re-syncs the generator after the server refused a msg_id as outside its window, to the server
     * second of the refusal, {@code replyTimeMillis}. A second which differs from the last one becomes
     * the time part, even when it is lower, and the sequence starts at a new lane. The same second keeps
     * the sequence, so no msg_id of that second is issued twice.
     * <p>
     * The msg_ids the server accepted before a re-sync to another second may still be recorded in the
     * seconds the time part covers again. The fresh lane makes a collision with them improbable, not impossible.
     */
    void resync(long replyTimeMillis) {
        long seconds = Math.min(replyTimeMillis / 1000L, MAX_SECONDS);
        if (seconds == lastSeconds) {
            return;
        }
        lastSeconds = seconds;
        lane = drawLane();
        lastSeq = lane - 1;
    }


    private long drawLane() {
        return random.nextInt(LANE_BOUND);
    }
}
