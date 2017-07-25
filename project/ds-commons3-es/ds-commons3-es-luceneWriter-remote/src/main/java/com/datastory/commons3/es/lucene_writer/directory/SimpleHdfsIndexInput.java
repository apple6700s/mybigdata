package com.datastory.commons3.es.lucene_writer.directory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * com.datastory.commons3.es.lucene_writer.directory.SimpleHdfsIndexInput
 *
 * @see org.apache.lucene.store.SimpleFSDirectory.SimpleFSIndexInput
 * @author lhfcws
 * @since 2017/6/7
 */
public class SimpleHdfsIndexInput extends BufferedIndexInput {
    /**
     * The maximum chunk size for reads of 16384 bytes.
     */
    private static final int CHUNK_SIZE = 16384;

    private FSDataInputStream in;
    boolean isClone = false;
    private long offset = 0l;
    private long length = 0l;
    private ByteBuffer byteBuf;

    protected SimpleHdfsIndexInput(FSDataInputStream in, long length) throws IOException {
        super(in.getFileDescriptor().toString());
        this.in = in;
        this.length = length;
    }

    public SimpleHdfsIndexInput(String resourceDescription, FSDataInputStream in, long offset, long length) {
        super(resourceDescription);
        this.in = in;
        this.offset = offset;
        this.length = length;
        this.isClone = true;
    }

    protected SimpleHdfsIndexInput(String desc, FSDataInputStream in, long length) throws IOException {
        super(desc);
        this.in = in;
        this.length = length;
    }

    @Override
    public void close() throws IOException {
        if (!isClone)
            in.close();
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public SimpleHdfsIndexInput clone() {
        SimpleHdfsIndexInput clone = (SimpleHdfsIndexInput)super.clone();
        clone.isClone = true;
        return clone;
    }

    @Override
    protected void newBuffer(byte[] newBuffer) {
        super.newBuffer(newBuffer);
        byteBuf = ByteBuffer.wrap(newBuffer);
    }

    @Override
    protected void readInternal(byte[] b, int offset, int length) throws IOException {
        in.read(b, offset, length);
        final ByteBuffer bb;

        // Determine the ByteBuffer we should use
        if (b == buffer) {
            // Use our own pre-wrapped byteBuf:
            assert byteBuf != null;
            bb = byteBuf;
            byteBuf.clear().position(offset);
        } else {
            bb = ByteBuffer.wrap(b, offset, length);
        }

        synchronized(in) {
            long pos = getFilePointer() + this.offset;

            if (pos + length > this.offset + this.length) {
                throw new EOFException("read past EOF: " + this);
            }

            try {
                in.seek(pos);

                int readLength = length;
                while (readLength > 0) {
                    final int toRead = Math.min(CHUNK_SIZE, readLength);
                    bb.limit(bb.position() + toRead);
                    assert bb.remaining() == toRead;
                    final int i = in.read(bb);
                    if (i < 0) { // be defensive here, even though we checked before hand, something could have changed
                        throw new EOFException("read past EOF: " + this + " off: " + offset + " len: " + length + " pos: " + pos + " chunkLen: " + toRead + " end: " + (this.offset + this.length));
                    }
                    assert i > 0 : "SeekableByteChannel.read with non zero-length bb.remaining() must always read at least one byte (Channel is in blocking mode, see spec of ReadableByteChannel)";
                    pos += i;
                    readLength -= i;
                }
                assert readLength == 0;
            } catch (IOException ioe) {
                throw new IOException(ioe.getMessage() + ": " + this, ioe);
            }
        }
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        in.seek(pos);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length()) {
            throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: "  + this);
        }
        return new SimpleHdfsIndexInput(getFullSliceDescription(sliceDescription), in, this.offset + offset, length);
    }
}
