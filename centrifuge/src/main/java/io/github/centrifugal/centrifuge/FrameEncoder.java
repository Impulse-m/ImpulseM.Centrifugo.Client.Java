package io.github.centrifugal.centrifuge;

import java.io.IOException;

import io.github.centrifugal.centrifuge.internal.protocol.Protocol;

import com.google.protobuf.CodedOutputStream;
import okio.ByteString;


/**
 * Encodes a command as one frame: the length prefix and the message, the bytes which
 * {@link Protocol.Command#writeDelimitedTo} writes. The frame is sized once and written into one array,
 * so a payload is copied from the command into the frame once, and the frame is copied once more into
 * the {@link ByteString} the socket takes. The stream of {@code writeDelimitedTo} grew its buffer as it
 * wrote, and copied it again on {@code toByteArray}: two copies of the frame fewer than before.
 * {@link Client} keeps the stream of {@code writeDelimitedTo} for its other sends, so the layout written here
 * must stay identical to what {@code Client.serializeCommand} writes.
 */
final class FrameEncoder {
    private FrameEncoder() {
    }


    /**
     * The frame of the command.
     *
     * @throws IOException when the command cannot be written.
     */
    static ByteString encode(Protocol.Command cmd) throws IOException {
        int size = cmd.getSerializedSize();
        byte[] frame = new byte[CodedOutputStream.computeUInt32SizeNoTag(size) + size];
        CodedOutputStream out = CodedOutputStream.newInstance(frame);
        out.writeUInt32NoTag(size);
        cmd.writeTo(out);
        out.checkNoSpaceLeft();
        return ByteString.of(frame);
    }
}
