//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.streams;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import java.io.*;
import java.util.Locale;

public class BzipFileOutputStream extends OutputStream implements Closeable, Flushable, Appendable {
    private final PrintStream out;

    public BzipFileOutputStream(String filename) throws IOException {
        FileOutputStream fs = new FileOutputStream(filename);
        out = new PrintStream(new BZip2CompressorOutputStream(fs));
    }

    public void write(int b) throws IOException {
        out.write(b);
    }

    public void write(byte[] b) throws IOException {
        out.write(b);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    public void flush() {
        out.flush();
    }

    public void close() {
        out.close();
    }

    public boolean checkError() {
        return out.checkError();
    }

    public void print(boolean b) {
        out.print(b);
    }

    public void print(char c) {
        out.print(c);
    }

    public void print(int i) {
        out.print(i);
    }

    public void print(long l) {
        out.print(l);
    }

    public void print(float f) {
        out.print(f);
    }

    public void print(double d) {
        out.print(d);
    }

    public void print(char[] s) {
        out.print(s);
    }

    public void print(String s) {
        out.print(s);
    }

    public void print(Object obj) {
        out.print(obj);
    }

    public void println() {
        out.println();
    }

    public void println(boolean x) {
        out.println(x);
    }

    public void println(char x) {
        out.println(x);
    }

    public void println(int x) {
        out.println(x);
    }

    public void println(long x) {
        out.println(x);
    }

    public void println(float x) {
        out.println(x);
    }

    public void println(double x) {
        out.println(x);
    }

    public void println(char[] x) {
        out.println(x);
    }

    public void println(String x) {
        out.println(x);
    }

    public void println(Object x) {
        out.println(x);
    }

    public PrintStream printf(String format, Object... args) {
        return out.printf(format, args);
    }

    public PrintStream printf(Locale l, String format, Object... args) {
        return out.printf(l, format, args);
    }

    public PrintStream format(String format, Object... args) {
        return out.format(format, args);
    }

    public PrintStream format(Locale l, String format, Object... args) {
        return out.format(l, format, args);
    }

    public PrintStream append(CharSequence csq) {
        return out.append(csq);
    }

    public PrintStream append(CharSequence csq, int start, int end) {
        return out.append(csq, start, end);
    }

    public PrintStream append(char c) {
        return out.append(c);
    }
}
