package java.io;

public interface DataOutput {
    void write(byte[] b) throws IOException;
    void write(byte[] b, int off, int len) throws IOException;
    void write(int b) throws IOException;
    void writeBoolean(boolean v) throws IOException;
    void writeByte(int v) throws IOException;
    void writeBytes(String s) throws IOException;
    void writeChar(int v) throws IOException;
    void writeChars(String s) throws IOException;
    void writeDouble(double v) throws IOException;
    void writeFloat(float v) throws IOException;
    void writeInt(int v) throws IOException;
    void writeLong(long v) throws IOException;
    void writeShort(int v) throws IOException;
    void writeUTF(String s) throws IOException;
}
