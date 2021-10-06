package io.deephaven.demo;

/**
 * NameConstants:
 * <p>
 * <p>
 * Instead of random magic strings littering random method bodies,
 * <p>
 * Let's instead litter this class with well-named constants ^-^
 */
public class NameConstants {

    public static final String NAME_DEPLOYMENT = "dh-deploy";
    public static final String NAMESPACE = "default";
    public static final String LABEL_USER = "dh-user";
    public static final String LABEL_PURPOSE = "dh-purpose";
    public static final String PURPOSE_WORKER = "worker";
    public static final String PURPOSE_CONTROLLER = "controller";
    public static final String PURPOSE_CREATOR = "creator";

    public static final String PORT_ENVOY_CLIENT_NAME = "envoy-client";
    public static final String ANNO_BACKEND_CONFIG = "beta.cloud.google.com/backend-config";
    public static final String BACKEND_ENVOY =
        "{\"ports\": { \"" + PORT_ENVOY_CLIENT_NAME + "\":\"dh-backend-envoy\" }}";
    public static final String ANNO_APP_PROTOCOLS = "cloud.google.com/app-protocols";
    public static final String PROTOCOL_HTTP2 = "{\"" + PORT_ENVOY_CLIENT_NAME + "\":\"HTTP2\"}";
    public static final String ANNO_NEG = "cloud.google.com/neg";
    public static final String NEG_INGRESS = "{\"ingress\": true}";

    public static final String PROP_HELM_ROOT = "dh-helm-root";
    public static final String PROP_HELM_TARGET = "dh-helm-target";
    public static final String PROP_HELM_INSTANCES = "dh-helm-instances";
    public static final String PROP_HELM_SUBDOMAINS = "dh-helm-subdomains";

    public static final int PORT_ENVOY_CLIENT =
        Integer.parseInt(System.getProperty("dh-envoy-port", "10000"));
    public static final String DH_HELM_MODE = System.getProperty("dh-helm-mode", "controller");
    public static final String DH_HELM_ROOT = System.getProperty(PROP_HELM_ROOT, ".");
    public static final String DH_HELM_TARGET =
        System.getProperty(PROP_HELM_TARGET, DH_HELM_ROOT + "/generated");
    public static final String DH_HELM_INSTANCES = System.getProperty(PROP_HELM_INSTANCES, "30");
    public static final String DH_HELM_SUBDOMAINS = System.getProperty(PROP_HELM_SUBDOMAINS, "");
    public static final String DH_NAMESPACE = System.getProperty("dh-namespace", "default");
    public static final String DH_DEPLOY_NAME = System.getProperty("dh-deploy-name", "dh-local");
    public static final String DH_INGRESS_NAME =
        System.getProperty("dh-ingress-name", "dh-ingress");
    public static final String DH_POD_KEY = System.getProperty("dh-pod-key", "dh-pod-id");
    public static final String REGION = System.getProperty("dh-region", "us-central1");
    public static final String VERSION = System.getProperty("dh-version", "0.5.0");
    public static final String VERSION_MANGLE = VERSION.replaceAll("[.]", "-");
    public static final String SNAPSHOT_NAME = System.getProperty("DH_SNAPSHOT_NAME", "deephaven-app-" + VERSION_MANGLE);
    public static final String DOMAIN;
    public static final String COOKIE_NAME = "dh-user";

    static {
        String domain = System.getenv("MY_DNS_NAME");
        String backup = "demo.deephaven.app";
        if (domain == null || domain.isEmpty()) {
            domain = backup;
            System.out.println("No MY_DNS_NAME env var set, defaulting to " + backup);
        }
        DOMAIN = domain;
    }
}
