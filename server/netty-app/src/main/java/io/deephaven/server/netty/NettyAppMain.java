package io.deephaven.server.netty;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class NettyAppMain {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {
        NettyMain.main(args);
    }
}
