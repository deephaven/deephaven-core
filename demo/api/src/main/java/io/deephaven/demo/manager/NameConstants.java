package io.deephaven.demo.manager;

/**
 * NameConstants:
 * <p>
 * <p>
 * Instead of random magic strings littering random method bodies,
 * <p>
 * Let's instead litter this class with well-named constants ^-^
 */
public class NameConstants {

    public static final String LABEL_USER = "dh-user";
    public static final String LABEL_PURPOSE = "dh-purpose";
    public static final String LABEL_IP_NAME = "dh-ip";
    public static final String LABEL_LEASE = "dh-lease";
    public static final String LABEL_VERSION = "dh-version";
    public static final String LABEL_DOMAIN = "dh-domain";
    public static final String PURPOSE_BASE = "base";
    public static final String PURPOSE_WORKER = "worker";
    public static final String PURPOSE_CONTROLLER = "controller";
    public static final String PURPOSE_CREATOR_CONTROLLER = "creator-" + PURPOSE_CONTROLLER;
    public static final String PURPOSE_CREATOR_WORKER = "creator-" + PURPOSE_WORKER;

    public static final String REGION = System.getProperty("dh-region", "us-central1");
    public static final String VERSION = System.getProperty("dh-version", System.getenv("VERSION"));
    static {
        if (VERSION == null) {
            throw new IllegalStateException("Must specify -Ddh-version= system property or export VERSION= environment variable");
        }
    }
    public static final String VERSION_MANGLE = VERSION.replaceAll("[.]", "-");
    public static final String SNAPSHOT_NAME = System.getProperty("DH_SNAPSHOT_NAME", "deephaven-app-" + VERSION_MANGLE);
    public static final String DOMAIN;
    public static final boolean CONTROLLER;
    public static final String COOKIE_NAME = "dh-user";

    static {
        CONTROLLER = "true".equals(System.getenv("IS_CONTROLLER"));
        System.out.println("CONTROLLER? " + System.getenv("IS_CONTROLLER") + " == true");
        String domain = System.getenv("MY_DNS_NAME");
        String backup = "demo.deephaven.app";
        if (domain == null || domain.isEmpty()) {
            domain = backup;
            System.out.println("No MY_DNS_NAME env var set, defaulting to " + backup);
        }
        DOMAIN = domain;
    }
}
