package io.github.centrifugal.centrifuge;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    public void unsupportedRpcReplyHasNoCommandIdAndCodeFifteen() {
        Protocol.Reply reply = Extensions.unsupportedRpcReply();

        assertEquals(0, reply.getId());
        assertEquals(ReplyError.UNSUPPORTED, reply.getError().getCode());
        assertEquals("rpc headers are not supported by the server", reply.getError().getMessage());
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
