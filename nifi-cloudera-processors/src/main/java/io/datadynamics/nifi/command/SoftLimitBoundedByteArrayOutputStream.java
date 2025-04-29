package io.datadynamics.nifi.command;

import java.io.IOException;
import java.io.OutputStream;

public class SoftLimitBoundedByteArrayOutputStream extends OutputStream {
    /*
     * This Bounded Array Output Stream (BAOS) allows the user to write to the output stream up to a specified limit.
     * Higher than that limit the BAOS will silently return and not put more into the buffer. It also will not throw an error.
     * This effectively truncates the stream for the user to fit into a bounded array.
     */

    private final byte[] buffer;
    private int limit;
    private int count;

    public SoftLimitBoundedByteArrayOutputStream(int capacity) {
        this(capacity, capacity);
    }

    public SoftLimitBoundedByteArrayOutputStream(int capacity, int limit) {
        if ((capacity < limit) || (capacity | limit) < 0) {
            throw new IllegalArgumentException("Invalid capacity/limit");
        }
        this.buffer = new byte[capacity];
        this.limit = limit;
        this.count = 0;
    }

    @Override
    public void write(int b) throws IOException {
        if (count >= limit) {
            return;
        }
        buffer[count++] = (byte) b;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
                || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        if (count + len > limit) {
            len = limit - count;
            if (len == 0) {
                return;
            }
        }

        System.arraycopy(b, off, buffer, count, len);
        count += len;
    }

    public void reset(int newlim) {
        if (newlim > buffer.length) {
            throw new IndexOutOfBoundsException("Limit exceeds buffer size");
        }
        this.limit = newlim;
        this.count = 0;
    }

    public void reset() {
        this.limit = buffer.length;
        this.count = 0;
    }

    public int getLimit() {
        return limit;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public int size() {
        return count;
    }
}
