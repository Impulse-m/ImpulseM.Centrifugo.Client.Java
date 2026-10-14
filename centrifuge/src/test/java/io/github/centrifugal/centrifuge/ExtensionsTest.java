package io.github.centrifugal.centrifuge;

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
}
