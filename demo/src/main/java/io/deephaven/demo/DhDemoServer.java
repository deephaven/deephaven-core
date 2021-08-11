package io.deephaven.demo;

import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.extended.kubectl.Kubectl;
import io.kubernetes.client.extended.kubectl.KubectlCreate;
import io.kubernetes.client.extended.kubectl.KubectlGet;
import io.kubernetes.client.extended.kubectl.KubectlPatch;
import io.kubernetes.client.extended.kubectl.exception.KubectlException;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ModelMapper;
import io.kubernetes.client.util.PatchUtils;
import io.kubernetes.client.util.Yaml;
import io.kubernetes.client.util.generic.options.ListOptions;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import org.joda.time.DateTime;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.cert.X509Certificate;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * DhDemoServer:
 * <p><p>
 *     Runs a simple vert.x frontend which acts as a controller for a given kubernetes cluster.
 * <p>
 * <p> This server will host the static deephaven demo server,
 * <p> and handle spinning up / connecting to a running worker.
 *
 */
@QuarkusMain
public class DhDemoServer {

    private static final String DH_NAMESPACE = System.getProperty("dh-namespace", "dh");
    private static final String DH_DEPLOY_NAME = System.getProperty("dh-deploy-name", "dh-local");
    private static final String DH_POD_KEY = System.getProperty("dh-pod-key", "dh-pod-id");

    public static void main(String ... args) throws IOException, InterruptedException {

        final List<String> leftover;
        if (args.length == 0) {
            leftover = Collections.emptyList();
        } else {
            leftover = Arrays.stream(args).skip(1).collect(Collectors.toList());
            switch (args[0]) {
                case "ensure-certs":
                    ensureCertificates(leftover);
                    break;
            }
        }

        Thread.setDefaultUncaughtExceptionHandler((thread, fail)->{
            System.err.println("Unhandled failure on thread " + thread.getName() + " (" + thread.getId() + ")");
            fail.printStackTrace();
        });

        Quarkus.run(args);

        // everything below does not run, Quarkus.run is terminal.
        // below is a previous experiment that has some useful code to move into proper vertx endpoints



        // For now, we are just going to render a single button which clicks to create a groovy / python session.
        final Vertx vertx = Vertx.vertx();
        HttpServer http = vertx.createHttpServer();
        http.requestHandler(request -> {

            HttpServerResponse response = request.response();
            String uri = request.uri();
            // TODO: use quarkus-friendly logging framework
            System.out.println("My uri: " + uri);
            try {
                final X509Certificate[] chain = request.peerCertificateChain();
                if (chain != null) {
                    System.out.println("My certs: " + Stream.of(chain)
                        .map(cert -> "cert {"
                                + "\n  " +
                                "pubKey: " + cert.getPublicKey()
                                + "\n  " +
                                "subject: " + cert.getSubjectDN()
                                + "\n" +
                                "}").collect(Collectors.joining(" -> ")));
                }
            } catch (SSLPeerUnverifiedException e) {
                e.printStackTrace();
            }
            try {

                if (uri.endsWith("/health")) {
                    // TODO: Replace this DIRTY CHEAT:
                    //    we need to actually talk to grpc-api before sending health status.
                    response.setStatusCode(200);
                    response.end();

                } else if (uri.endsWith("/python")) {
                    // start a python session, and send the user to their new URL
                    startPythonSession(request);
                } else if (uri.endsWith("/groovy")) {
                    // start a groovy session, and send the user to their new URL
                    startGroovySession(request);
                } else {
                    // This handler gets called for each request that arrives on the server
                    response.putHeader("content-type", "text/html");
                    // Write to the response and end it
                    response.end("<!DOCTYPE html><html><body>" +
                            "<a target='_blank' href='/python'>Start Python</a>" +
                            "<br/>" +
                            "<a target='_blank' href='/groovy'>Start Groovy</a>" +
                            "</body></html>");
                }
            } catch (IOException | ApiException | KubectlException e) {
                final UUID trackingId = UUID.randomUUID();
                final String sessionInfo = dumpSession(request);
                synchronized (System.err) {
                    System.err.print("TrackingID: " + trackingId + "\n");
                    System.err.println(sessionInfo);
                    e.printStackTrace(System.err);
                }
                System.err.flush();
                response.putHeader("content-type", "text/html");
                // Write to the response and end it
                response.end("<!DOCTYPE html><html><body>" +
                            "An error occurred on the server; please file a bug report at <a href='https://github.com/deephaven/deephaven-core/issues'>https://github.com/deephaven/deephaven-core/issues</a>" +
                            "<br />Include error code <span style='color:red'>" + trackingId + "</span>" +
                            "</body></html>");

            }
        });

        http.listen(8080);
        System.out.println("Server is listening on http://127.0.0.1:8080");
    }

    private static void ensureCertificates(final List<String> extraArgs) throws IOException, InterruptedException {
        // actually generate any necessary certificates
        CertFactory.main(extraArgs.toArray(new String[0]));
    }

    private static ExecutorService executor = Executors.newFixedThreadPool(6);

    private static void startGroovySession(final HttpServerRequest request) throws IOException, ApiException, KubectlException {
        startLanguageSession("Groovy", request);
    }

    private static void startPythonSession(final HttpServerRequest request) throws IOException, ApiException, KubectlException {
        startLanguageSession("Python", request);
    }

    private static void startLanguageSession(String lang, final HttpServerRequest request) {
        // get off the vert.x event queue
        executor.submit(()-> {
            final String url;
            try {
                url = getOrCreateLanguageSession(lang, request);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
            // reenter the vertx context to deliver the session
            Vertx.currentContext().runOnContext(v->{
                request.response().putHeader("location", url)
                        .setStatusCode(302)
                        .end();
            });
            return url;
        });
    }
    private static String getOrCreateLanguageSession(String lang, final HttpServerRequest request) throws IOException, ApiException, KubectlException, InterruptedException {
        // alright, start up a groovy worker, and send the user to it via redirect
        System.out.println("Starting a " + lang + " session for " + dumpSession(request));
        String url = "https://deephaven.io";

        // ok, tell kube cluster to spin up a new host!

        CoreV1Api api = KubeTools.getApi();
        // ok, first, looks for any pods which are advertising room for new sessions (TODO: use a label / health probe to find existing pods)
        V1Pod chosenOne = findChosenOne(api, request);
        long backoff = 100;

        if (chosenOne == null) {
            ensureNamespace(api);
            // if nothing is found, create a new pod to connect to
            int machineCnt = countMachines(api);
            // now, we should really wait on this to scale up!
            int newMachines = doScaleUp(api, request);
            int nextCnt;
            while ( machineCnt < newMachines) {
                nextCnt = countMachines(api);
                if (nextCnt != machineCnt) {
                    // try to get a chosenOne once machine count changes
                    chosenOne = findChosenOne(api, request);
                    if (chosenOne == null) {
                        // lets back off a little...
                        Thread.sleep(backoff += 100);
                    } else {
                        break;
                    }
                }
                machineCnt = nextCnt;
            }
        }
        // The chosen one has been found!
        // TODO: extract the path to this pod as our url-to-redirect-to
        url = "http://" + chosenOne.getSpec().getHostname() + ":8765/ide/"; // this is almost certainly wrong.

//        for (V1Pod item : list.getItems()) {
//            System.out.println(item.getMetadata().getName());
//        }
        return url;
    }

    private static V1Pod findChosenOne(final CoreV1Api api, final HttpServerRequest request) throws ApiException {
        V1PodList list = api.listNamespacedPod(DH_NAMESPACE, null, null, null, "metadata.name=dh-local", "run=dh-local", null, null, 10, false);
        findTheOne:
        for (V1Pod item : list.getItems()) {
            // check if this pod has room for us.
            final V1PodStatus status = item.getStatus();
            if (status == null) {
                continue;
            }
            final DateTime started = status.getStartTime();
            // TODO: decide how long to keep pods around before we stop scheduling to them and let them shut down.
            // Keeping kubernetes pods indefinitely has not, historically, worked well.

            final List<V1ContainerStatus> containerStats = status.getContainerStatuses();
            if (containerStats == null) {
                continue;
            }
            for (V1ContainerStatus containerStatus : containerStats) {
                if (!containerStatus.getReady()) {
                    continue findTheOne;
                }
            }
            // TODO: something smart, where we actually query the state of the pod before sending it work.
            final V1ObjectMeta metadata = item.getMetadata();
            if (metadata == null) {
                continue;
            }

            Map<String, String> labels = metadata.getLabels();
            if (labels == null) {
                labels = new HashMap<>();
                metadata.setLabels(labels);
            }
            final String used = labels.get(DH_POD_KEY);
            if (used != null) {
                // TODO: consider reusing existing pod if it matches vert.x session id?
                // ...we don't have sessions there hooked up, but this is where we'd do it
                continue;
            }

            // now, try to set the pod key to user request.
            int streamId = request.streamId();
            String key = "req-" + streamId;
            labels.put(DH_POD_KEY, key);
            String patchLabel = "metadata:\n" +
                    "  labels:\n" +
                    "    ";
            final V1Patch labelPatch = new V1Patch(patchLabel);
            final V1Pod patchResult = api.patchNamespacedPod(metadata.getName(), metadata.getNamespace(), labelPatch, null, null, null, null);
            final String actual = patchResult.getMetadata().getLabels().get(DH_POD_KEY);
            if (!key.equals(actual)) {
                System.out.println("We lost a contentious grab for pod " + item.getMetadata().getName());
                continue;
            }
            // we were able to set the label to our unique id, it's ours!
            return item;
        }
        return null;
    }

    private static int countMachines(final CoreV1Api api) throws ApiException {
        final V1PodList results = api.listNamespacedPod(DH_NAMESPACE, null, null, null, "metadata.name=dh-local", null, 10_000, null, 2, null);
        final List<V1Pod> items = results.getItems();
        return items.size();
    }

    private static V1Pod newPodSpec(final CoreV1Api api, final HttpServerRequest request) throws KubectlException {
        final V1Pod pod = new V1Pod();
        pod.setApiVersion("v1");
        return pod;
    }

    private static int doScaleUp(final CoreV1Api api, final HttpServerRequest request) throws KubectlException, ApiException, IOException {
//        ensureNamespace(api);
        AppsV1Api appsApi = new AppsV1Api(api.getApiClient());
        final Class<?> apiType = ModelMapper.preBuiltGetApiTypeClass("", "v1", "Deployment");
        KubectlGet<V1Deployment> getDeploy = Kubectl.get(V1Deployment.class)
                .namespace(DH_NAMESPACE)
                .apiListTypeClass(V1DeploymentList.class)
                ;
        getDeploy.apiClient(api.getApiClient());
        final ListOptions opts = new ListOptions();
        getDeploy.options(opts);


        V1Deployment deployment = null;
        try {
            final List<V1Deployment> result = getDeploy.execute();
            if (!result.isEmpty()) {
                deployment = result.get(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (deployment == null) {
            // try to create the deployment.
            final KubectlCreate<V1Deployment> createDeploy = Kubectl.create(V1Deployment.class);

            final Map<String, String> labelMatch = new HashMap<>();
            labelMatch.put("run", "dh-local");
            final V1Deployment fromYaml = Yaml.loadAs(new File("/dh/ws0/deephaven-core/demo/src/main/resources/io/deephaven/demo/dh-localhost.yaml"), V1Deployment.class);
            fromYaml.getSpec().setReplicas(1);

            final V1Deployment result = createDeploy
                    .resource(fromYaml)
                    .namespace(DH_NAMESPACE)
                    .name(DH_DEPLOY_NAME)
                    .apiClient(api.getApiClient())
                    .execute();
            System.out.println(result == fromYaml);
            System.out.println("Created deployment " + result.getSpec());
            deployment = result;

        }
        V1DeploymentSpec spec = deployment.getSpec();
        if (spec == null) {
            spec = new V1DeploymentSpec();
            deployment.setSpec(spec);
        }
        Integer curReps = spec.getReplicas();
        int numAdd = 1;
        if (curReps == null) {
            curReps = 0;
        } else if (curReps > 10) {
            numAdd = 4;
            if (curReps > 100) {
                numAdd = 10;
            }
        }
        int newReps = curReps + numAdd;
        spec.setReplicas(numAdd);

        final KubectlPatch<V1Deployment> patchDeploy = Kubectl.patch(V1Deployment.class);

        patchDeploy.apiClient(api.getApiClient().setDebugging(true));
        String patchContent =
//                "apiVersion: apps/v1\n" +
//                "type: Deployment\n" +
                "spec:\n" +
                "  replicas: " + newReps;

        final V1Patch patchUpdate = new V1Patch(patchContent);

//        patchDeploy
//                .namespace(DH_NAMESPACE)
//                .name(DH_DEPLOY_NAME)
//                .patchContent(patchUpdate);
//        final V1Deployment result = patchDeploy.execute();

        final PatchUtils.PatchCallFunc a;
        final String b;
        final ApiClient c;
        V1Deployment res = PatchUtils.patch(V1Deployment.class, () -> appsApi.patchNamespacedDeploymentCall(DH_DEPLOY_NAME, DH_NAMESPACE,
                patchUpdate, null, null, "kubectl", null, null
        ), V1Patch.PATCH_FORMAT_APPLY_YAML, appsApi.getApiClient());
        System.out.println("Scale up result: " + res);
        return newReps;
    }

    private static void ensureNamespace(final CoreV1Api api) throws ApiException {
        V1NamespaceList ns = getNs(api);
        if (ns.getItems().isEmpty()) {
            final V1Namespace newNs = new V1NamespaceBuilder()
                    .withApiVersion("v1")
                    .withNewMetadata()
                        .withName(DH_NAMESPACE)
                        .endMetadata()
                    .build();
            api.createNamespace(newNs, null, null, null);
            while (getNs(api) == null) {
                System.out.println("Still waiting on namespace " + DH_NAMESPACE + " to be created");
            }
        }
    }

    private static V1NamespaceList getNs(final CoreV1Api api) throws ApiException {
        return api.listNamespace(null, null, null, "metadata.name=" + DH_NAMESPACE, null, null, null, 2, null);
    }

    private static String dumpSession(final HttpServerRequest request) {
        return request.absoluteURI() + " with headers:\n" +
                request.headers().entries().stream().map(e->
                        e.getKey() + "=" + e.getValue()
                ).collect(Collectors.joining("\n"));
    }
}
