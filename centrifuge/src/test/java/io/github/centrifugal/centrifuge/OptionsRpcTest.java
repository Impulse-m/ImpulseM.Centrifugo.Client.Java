package io.github.centrifugal.centrifuge;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/** Tests of the options of the exactly-once rpc calls: the connection instance and the queue bound. */
public class OptionsRpcTest {
    @Test
    public void instanceIsEmptyByDefaultAndNullClearsIt() {
        Options opts = new Options();
        assertEquals("", opts.getInstance());

        opts.setInstance("abcdefghijklmnopqrstuv");
        opts.setInstance(null);

        assertEquals("", opts.getInstance());
    }


    @Test
    public void setInstanceKeepsAValidValue() {
        Options opts = new Options();
        opts.setInstance("abcdefghijklmnopqrstuv");
        assertEquals("abcdefghijklmnopqrstuv", opts.getInstance());
    }


    @Test
    public void setInstanceRejectsAValueOutsideThePattern() {
        Options opts = new Options();
        try {
            opts.setInstance("too-short");
            fail("expected the instance to be rejected");
        } catch (IllegalArgumentException expected) {
            assertEquals("", opts.getInstance());
        }
    }


    @Test
    public void maxPendingRpcDefaultsToThirtyTwo() {
        assertEquals(32, new Options().getMaxPendingRpc());
    }


    @Test
    public void setMaxPendingRpcRejectsABoundBelowOne() {
        Options opts = new Options();
        try {
            opts.setMaxPendingRpc(0);
            fail("expected the bound to be rejected");
        } catch (IllegalArgumentException expected) {
            assertEquals(32, opts.getMaxPendingRpc());
        }
    }
}
