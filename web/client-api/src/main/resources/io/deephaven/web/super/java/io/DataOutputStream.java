package java.io;

import org.gwtproject.nio.Numbers;

import java.nio.charset.StandardCharsets;

public class DataOutputStream extends FilterOutputStream implements DataOutput {
    protected int written;

    public DataOutputStream(OutputStream out) {
        super(out);
    }

    @Override
    public void write(int b) throws IOException {
        super.write(b);
        written++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        super.write(b, off, len);
        written += len;
    }

    @Override
    public void writeBoolean(boolean b) throws IOException {
        write(b ? 1 : 0);
    }

    @Override
    public void writeByte(int i) throws IOException {
        write(i);
    }

    @Override
    public void writeShort(int i) throws IOException {
        out.write((i >> 8) & 0xFF);
        out.write((i >> 0) & 0xFF);
        written += 2;
    }

    @Override
    public void writeChar(int i) throws IOException {
        out.write((i >> 8) & 0xFF);
        out.write((i >> 0) & 0xFF);
        written += 2;
    }

    @Override
    public void writeInt(int i) throws IOException {
        out.write((i >> 24) & 0xFF);
        out.write((i >> 16) & 0xFF);
        out.write((i >> 8) & 0xFF);
        out.write((i >> 0) & 0xFF);
        written += 4;
    }

    @Override
    public void writeLong(long l) throws IOException {
        out.write((int) (l >> 56) & 0xFF);
        out.write((int) (l >> 48) & 0xFF);
        out.write((int) (l >> 40) & 0xFF);
        out.write((int) (l >> 32) & 0xFF);
        out.write((int) (l >> 24) & 0xFF);
        out.write((int) (l >> 16) & 0xFF);
        out.write((int) (l >> 8) & 0xFF);
        out.write((int) (l >> 0) & 0xFF);
        written += 8;
    }

    @Override
    public void writeFloat(float v) throws IOException {
        writeInt(Numbers.floatToIntBits(v));
    }

    @Override
    public void writeDouble(double v) throws IOException {
        writeLong(Numbers.doubleToRawLongBits(v));
    }

    @Override
    public void writeBytes(String s) throws IOException {
        for (int i = 0; i < s.length(); i++) {
            out.write(s.charAt(i) & 0xFF);
        }
        written += s.length();
    }

    @Override
    public void writeChars(String s) throws IOException {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            out.write((c >> 8) & 0xFF);
            out.write(c & 0xFF);
        }
        written += s.length() * 2;
    }

    @Override
    public void writeUTF(String s) throws IOException {
        throw new UnsupportedOperationException("modified utf-8");
    }
}
