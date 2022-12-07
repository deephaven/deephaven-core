package io.deephaven.server.console.completer;

import org.jpy.PyObject;

import java.io.Closeable;

public interface JediCompleter extends Closeable {

    void open_doc(String text, String uri, int version);

    String get_doc(String uri);

    void update_doc(String document, String uri, int version);

    void close_doc(String uri);

    PyObject do_completion(String uri, int version, int line, int character);

    @Override
    void close();
}
