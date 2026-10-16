package io.github.centrifugal.centrifuge;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import com.google.protobuf.ByteString;

import static org.junit.Assert.assertArrayEquals;


public class FrameEncoderTest {

    @Test
    public void aSmallCommandIsTheDelimitedBytes() throws IOException {
        assertSameAsDelimited(command(new byte[]{1, 2, 3}));
    }


    @Test
    public void anEmptyPayloadIsTheDelimitedBytes() throws IOException {
        assertSameAsDelimited(command(new byte[0]));
    }


    @Test
    public void aMegabytePayloadIsTheDelimitedBytes() throws IOException {
        byte[] payload = new byte[1024 * 1024];
        Arrays.fill(payload, (byte) 7);
        assertSameAsDelimited(command(payload));
    }


    private static void assertSameAsDelimited(Protocol.Command cmd) throws IOException {
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        cmd.writeDelimitedTo(expected);
        assertArrayEquals(expected.toByteArray(), FrameEncoder.encode(cmd).toByteArray());
    }


    private static Protocol.Command command(byte[] payload) {
        Protocol.RPCRequest rpc = Protocol.RPCRequest.newBuilder()
                .setMethod("blob.writePart")
                .setData(ByteString.copyFrom(payload))
                .build();
        return Protocol.Command.newBuilder()
                .setId(1)
                .setRpc(rpc)
                .build();
    }
}
