package io.deephaven.base.testing;

/**
 * A "fishlib JUnit" compatible test class with a timeout that can fail tests after a given timeout expires.
 *
 * Override {@link #maxMillis()} to configure.
 *
 * If you are writing any new tests using junit 4+, instead use @Test(timeout=15_000) to set timeouts directly on each
 * method.
 */
public abstract class TimeLimitedTest extends BaseCachedJMockTestCase {

    private Thread timeout;

    @Override
    @SuppressWarnings("deprecation")
    protected void setUp() throws Exception {
        final Thread running = Thread.currentThread();
        final long ttl = maxMillis();
        final long deadline = System.currentTimeMillis() + ttl;
        timeout = new Thread(() -> {
            while (System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(deadline - System.currentTimeMillis());
                } catch (InterruptedException e) {
                    return; // test finished normally (tearDown() was called)
                }
            }
            // test has timed out.
            running.interrupt();
            try {
                running.join(5_000);
            } catch (InterruptedException e) {
                // maybe normal completion
                if (running.isAlive()) {
                    // nope; we're stuck... force kill.
                    // stop() is deprecated and dangerous,
                    // but it's still better than a deadlocked VM staying alive for hours
                    System.err.println("Force killing thread after exceeding " + ttl + " ms");
                    new IllegalStateException()
                            .printStackTrace();
                    running.stop();
                }
            }
        });
        timeout.setName("TestReaper");
        timeout.setDaemon(true);
        timeout.start();
    }

    protected long maxMillis() {
        return 15_000;
    }

    @Override
    protected void tearDown() throws Exception {
        timeout.interrupt();
        super.tearDown();
    }


}
