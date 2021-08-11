package io.deephaven.demo;

import io.kubernetes.client.Discovery;
import io.kubernetes.client.extended.kubectl.exception.KubectlException;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.ModelMapper;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;

import static io.kubernetes.client.util.ClientBuilder.kubeconfig;
import static io.kubernetes.client.util.KubeConfig.loadKubeConfig;

/**
 * KubeTools:
 * <p><p>
 */
public class KubeTools {

    private static final ApiClient apiClient;
    static {
        // file path to your KubeConfig

        String kubeConfigPath = System.getenv("HOME") + "/.kube/config";

        if (new File(kubeConfigPath).exists()) {
            // load kubeconfig from file-system
            try {
                apiClient = kubeconfig(loadKubeConfig(new FileReader(kubeConfigPath))).build();
            } catch (IOException e) {
                // static initializer exceptions can get lost, so make sure we're chatty about stack traces
                e.printStackTrace();
                throw new UncheckedIOException(e);
            }
            // set the global default api-client to the in-cluster one from above
            Configuration.setDefaultApiClient(apiClient);

            try {
                ModelMapper.refresh(new Discovery(apiClient));
            } catch (ApiException e) {
                System.err.println("Failed to run discovery on kubernetes apiClient:\n" +
                        "code: " + e.getCode() + " response: " + e.getResponseBody());
                e.printStackTrace();
                throw new RuntimeException(e);
            }

        } else {
            System.out.println("No " + kubeConfigPath + " file found; not loading context from filesystem");
            apiClient = Configuration.getDefaultApiClient();
        }
    }

    public static CoreV1Api getApi() {
        final CoreV1Api api = new CoreV1Api(apiClient);
        api.getApiClient().setDebugging(true);
        return api;
    }
}
