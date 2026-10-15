package io.github.centrifugal.centrifuge;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/** Unit tests of the helpers of the exactly-once rpc calls: the instance pattern and the reply of a call not sent. */
public class ExtensionsExactlyOnceTest {
    @Test
    public void unsupportedRpcReplyWithoutIdHasNoCommandIdAndCodeFifteen() {
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
