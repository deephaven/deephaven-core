package io.deephaven.server.jetty;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class JettyAppMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {
        JettyMain.main(args);
    }
}
