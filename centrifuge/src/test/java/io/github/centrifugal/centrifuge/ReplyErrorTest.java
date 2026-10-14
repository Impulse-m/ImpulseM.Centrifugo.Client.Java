package io.github.centrifugal.centrifuge;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ReplyErrorTest {
    @Test
    public void nullApplicationFieldsAreTreatedAsAbsent() {
        ReplyError error = new ReplyError(107, "bad request", false, 0, null, null);

        assertEquals(0, error.getAppCode());
        assertEquals("", error.getAppType());
        assertTrue(error.getMetadata().isEmpty());
    }
}
