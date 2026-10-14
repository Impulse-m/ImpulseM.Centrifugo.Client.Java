package io.github.centrifugal.centrifuge;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


/** Unit tests of the arithmetic behind the protocol extensions. */
public class ExtensionsTest {
    @Test
    public void rpcOptionsFlagIsBit32() {
        assertEquals(4294967296L, Extensions.RPC_OPTIONS_FLAG);
    }


    @Test
    public void hasFlagFindsBit32AmongOtherBits() {
        assertTrue(Extensions.hasFlag((1L << 32) | 1L, Extensions.RPC_OPTIONS_FLAG));
        assertTrue(Extensions.hasFlag(1L << 32, Extensions.RPC_OPTIONS_FLAG));
    }


    @Test
    public void hasFlagIsFalseWithoutTheBit() {
        assertFalse(Extensions.hasFlag(0L, Extensions.RPC_OPTIONS_FLAG));
        assertFalse(Extensions.hasFlag(1L, Extensions.RPC_OPTIONS_FLAG));
        assertFalse(Extensions.hasFlag(1L << 31, Extensions.RPC_OPTIONS_FLAG));
    }


    @Test
    public void serverTimeOffsetIsServerTimeMinusLocalTime() {
        assertEquals(Long.valueOf(500L), Extensions.serverTimeOffsetFrom(1_000_500L, 1_000_000L));
        assertEquals(Long.valueOf(-250L), Extensions.serverTimeOffsetFrom(999_750L, 1_000_000L));
    }


    @Test
    public void zeroServerTimeMeansNoOffset() {
        assertNull(Extensions.serverTimeOffsetFrom(0L, 1_000_000L));
    }


    @Test
    public void estimatedServerTimeAddsOffsetToLocalTime() {
        assertEquals(1_000_500L, Extensions.estimateServerTime(1_000_000L, 500L));
        assertEquals(999_750L, Extensions.estimateServerTime(1_000_000L, -250L));
    }


    @Test
    public void estimatedServerTimeIsLocalTimeWithoutOffset() {
        assertEquals(1_000_000L, Extensions.estimateServerTime(1_000_000L, 0L));
    }


    @Test
    public void unsupportedRpcReplyAnswersTheCommandWithCodeFifteen() {
        Protocol.Reply reply = Extensions.unsupportedRpcReply(7);

        assertEquals(7, reply.getId());
        assertEquals(ReplyError.UNSUPPORTED, reply.getError().getCode());
        assertEquals("rpc headers are not supported by the server", reply.getError().getMessage());
    }


    @Test
    public void msgIdCarriesTheUnixSecondAndStartsSequenceAtZero() {
        assertEquals(1_700_000_000L << 20, Extensions.nextMsgId(1_700_000_000_999L, 0L));
    }


    @Test
    public void msgIdIncrementsWithinOneSecond() {
        long first = Extensions.nextMsgId(1_700_000_000_100L, 0L);

        assertEquals(first + 1, Extensions.nextMsgId(1_700_000_000_900L, first));
    }


    @Test
    public void msgIdStaysIncreasingWhenTheClockGoesBack() {
        long last = Extensions.nextMsgId(1_700_000_005_000L, 0L);

        assertEquals(last + 1, Extensions.nextMsgId(1_700_000_001_000L, last));
    }


    @Test
    public void msgIdFitsFiftyThreeBitsForTodaysClock() {
        long msgId = Extensions.nextMsgId(1_800_000_000_000L, 0L);

        assertTrue(msgId <= Extensions.MSG_ID_MAX);
        assertTrue(msgId > 0L);
    }


    @Test
    public void instanceAcceptsTwentyTwoToSixtyFourUrlSafeCharacters() {
        assertTrue(Extensions.isValidInstance("abcdefghijklmnopqrstuv"));
        assertTrue(Extensions.isValidInstance("ABCDEFGHIJKLMNOPQRSTUV_-"));
        assertTrue(Extensions.isValidInstance(repeat('x', 64)));
    }


    @Test
    public void instanceRejectsOtherLengthsAndCharacters() {
        assertFalse(Extensions.isValidInstance(repeat('x', 21)));
        assertFalse(Extensions.isValidInstance(repeat('x', 65)));
        assertFalse(Extensions.isValidInstance("abcdefghijklmnopqrstu+"));
        assertFalse(Extensions.isValidInstance("abcdefghijklmnopqrstuv\n"));
        assertFalse(Extensions.isValidInstance(""));
    }


    private static String repeat(char c, int count) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            builder.append(c);
        }
        return builder.toString();
    }
}
