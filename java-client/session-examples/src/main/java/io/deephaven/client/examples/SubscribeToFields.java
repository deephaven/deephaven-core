package io.deephaven.client.examples;

import io.deephaven.client.impl.ApplicationService.Cancel;
import io.deephaven.client.impl.ApplicationService.Listener;
import io.deephaven.client.impl.Session;
import io.deephaven.proto.backplane.grpc.FieldInfo;
import io.deephaven.proto.backplane.grpc.FieldsChangeUpdate;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.concurrent.CountDownLatch;

@Command(name = "subscribe-fields", mixinStandardHelpOptions = true,
        description = "Subscribe to fields", version = "0.1.0")
public final class SubscribeToFields extends SingleSessionExampleBase {
    @Override
    protected void execute(Session session) throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final Cancel cancel = session.subscribeToFields(new Listener() {
            @Override
            public void onNext(FieldsChangeUpdate fields) {
                System.out.println("Created: " + fields.getCreatedCount());
                System.out.println("Updated: " + fields.getUpdatedCount());
                System.out.println("Removed: " + fields.getRemovedCount());
                for (FieldInfo fieldInfo : fields.getCreatedList()) {
                    System.out.println("Created: " + fieldInfo);
                }
                for (FieldInfo fieldInfo : fields.getUpdatedList()) {
                    System.out.println("Updated: " + fieldInfo);
                }
                for (FieldInfo fieldInfo : fields.getRemovedList()) {
                    System.out.println("Removed: " + fieldInfo);
                }
            }

            @Override
            public void onError(Throwable t) {
                if (!isCancelled(t)) {
                    t.printStackTrace(System.err);
                }
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });
        Runtime.getRuntime().addShutdownHook(new Thread(cancel::cancel));
        latch.await();
    }

    private static boolean isCancelled(Throwable t) {
        if (t instanceof StatusRuntimeException) {
            return ((StatusRuntimeException) t).getStatus().getCode() == Code.CANCELLED;
        } else if (t instanceof StatusException) {
            return ((StatusException) t).getStatus().getCode() == Code.CANCELLED;
        }
        return false;
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new SubscribeToFields()).execute(args);
        System.exit(execute);
    }
}
