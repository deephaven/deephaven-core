package java.io;

public interface DataInput {
    boolean readBoolean() throws IOException;
    byte readByte() throws IOException;
    char readChar() throws IOException;
    double readDouble() throws IOException;
    float readFloat() throws IOException;
    void readFully(byte[] b) throws IOException;
    void readFully(byte[] b, int off, int len) throws IOException;
    int readInt() throws IOException;
    String readLine() throws IOException;
    long readLong() throws IOException;
    short readShort() throws IOException;
    int readUnsignedByte() throws IOException;
    int readUnsignedShort() throws IOException;
    String readUTF() throws IOException;
    int skipBytes(int n) throws IOException;
}
