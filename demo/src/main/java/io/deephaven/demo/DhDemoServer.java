package io.deephaven.demo;

import io.deephaven.demo.deploy.GoogleDeploymentManager;
import io.deephaven.demo.deploy.Machine;
import io.kubernetes.client.custom.IntOrString;
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
import io.kubernetes.client.util.PatchUtils;
import io.kubernetes.client.util.Yaml;
import io.kubernetes.client.util.generic.options.ListOptions;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.vertx.core.http.Cookie;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.mutiny.core.Vertx;
import org.jboss.logging.Logger;

import javax.enterprise.inject.spi.CDI;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static io.deephaven.demo.NameConstants.*;

/**
 * DhDemoServer:
 * <p>
 * <p>
 * Runs a simple vert.x frontend which acts as a controller for a given kubernetes cluster.
 * <p>
 * <p>
 * This server will host the static deephaven demo server,
 * <p>
 * and handle spinning up / connecting to a running worker.
 *
 */
@QuarkusMain
public class DhDemoServer implements QuarkusApplication {

    private static final Logger LOG = Logger.getLogger(DhDemoServer.class);

    private final Vertx vertx;


    @Inject
    public DhDemoServer(Vertx vertx) {
        this.vertx = vertx;
    }

    public static void main(String... args)
        throws IOException, InterruptedException, ApiException, KubectlException {

        if (args.length > 0) {
            final List<String> leftover;
            leftover = Arrays.stream(args).skip(1).collect(Collectors.toList());
            switch (args[0]) {
                case "ensure-certs":
                    ensureCertificates(leftover);
                    return;
                case "generate":
                    generateHelm(leftover);
                    return;
            }
        }
        if ("generate".equals(DH_HELM_MODE)) {
            generateHelm(Arrays.asList(args));
            return;
        }
        Quarkus.run(DhDemoServer.class, args);
    }

    private static void generateHelm(final List<String> leftover) throws IOException {
        File helmRoot = new File(DH_HELM_ROOT);
        File helmTarget = new File(DH_HELM_TARGET);

        // alright! we have a templates directory, and a place to put modified yaml.
        new HelmGenerator(helmRoot, helmTarget).generate();

    }

    @Override
    public int run(final String... args) throws Exception {
        System.out.println("Setting up Deephaven Demo Server!");
        Thread.setDefaultUncaughtExceptionHandler((thread, fail) -> {
            System.err.println(
                "Unhandled failure on thread " + thread.getName() + " (" + thread.getId() + ")");
            fail.printStackTrace();
        });

        Router router = CDI.current().select(Router.class).get();
//        KubeManager state = new KubeManager();
        controller = new ClusterController(new GoogleDeploymentManager("/tmp"));
        router.get("/health").handler(rc -> {
            System.out.println("Health check! " + rc.request().uri() + " : " +
                rc.request().headers().get("User-Agent") + " ( "
                + rc.request().headers().get("host") + " ) ");

            // TODO: real health check
            //    for workers, grpcurl is on cli, or we can use grpc client here in java
            //    for controllers, we should check if we have fresh metadata and no fatal errors.
            // TODO: handle built-in quarkus health check
            rc.response()
                    .putHeader("Access-Control-Allow-Origin", "https://" + DOMAIN)
                    .putHeader("Access-Control-Allow-Methods", "GET")
                    .end("READY");
        });
        router.get("/robots.txt").handler(rc -> {
            LOG.info("ROBOT DETECTED! " + rc.request().uri() + " : " +
                rc.request().headers().get("User-Agent") + " ( "
                + rc.request().headers().get("host") + " ) ");
            rc.response()
                    .putHeader("Access-Control-Allow-Origin", "https://" + DOMAIN)
                    .putHeader("Access-Control-Allow-Methods", "GET")
                    // disallow all robots...
                    .end("User-agent: *\n" +
                            "Disallow: /");
        });
        router.get().handler(req -> {

            // for now, we are temporarily skipping ssl...
//            if (!req.request().isSSL()) {
//                System.out.println("Redirecting non-ssl request " + req.normalizedPath());
//                String domain = req.request().uri();
//                String uri = domain.contains(":") ? domain : "https://" + domain;
//                req.response()
//                    .putHeader("Location", uri)
//                    .setStatusCode(302)
//                    .end("<!html><html><body>" +
//                            "You are being redirected to a secure protocol: <a href=\"" + uri + "\">" + uri + "</a>" +
//                            "</body></html>");
//                return;
//            }
            String userAgent = req.request().headers().get("User-Agent");
            // We've seen Nimbostratus and SlackBot in logs; pre-emptively using "Bot" to cover more than SlackBot
            if (userAgent.contains("Bot") || userAgent.contains("Nimbostratus")) {
                LOG.info("Rejecting bot: " + userAgent);
                req.request().headers().entries().forEach(e->{
                    LOG.info("Header: " + e.getKey() + " = " + e.getValue());
                });
                req.end();
                return;
            }
            LOG.info("Handling " + req.request().method() + " " + req.normalizedPath() + " from " + userAgent);
            if (req.request().path() != null && req.request().path().endsWith("favicon.ico")) {
                // TODO: actually serve our favicon
                LOG.info("Skipping do-nothing favicon.ico");
                req.response().setStatusCode(200).end();
                return;
            }
            Cookie cookie = req.getCookie(NameConstants.COOKIE_NAME);
            if (cookie != null) {
                String uname = cookie.getValue();
                LOG.info("Handling request " + req.request().uri());
                // verify that this cookie points to a running service, and if so, redirect to it.

                if (controller.isMachineReady(uname)) {
                    if (!uname.contains(".")) {
                        uname = uname + "." + DOMAIN;
                    }
                    String uri = "https://" + uname;
                    req.redirect(uri);
                    return;
                }
            }
            // getting or creating a worker could take a while.
            // for now, we're going to let the browser window hang while we wait :'(
            // get off the vert.x event queue...
            LOG.info("Sending user off thread to complete new machine request");
            ClusterController.setTimer("Send to new machine", () -> {

                // not using kube for now
                // handleKube(req);

                LOG.info("Finding gcloud worker for user");
                handleGcloud(req);
            });

        });
        LOG.info("Ready to start serving https://" + DOMAIN);
        Quarkus.waitForExit();
        return 0;
    }

    private void handleGcloud(final RoutingContext req) {
//        if (pool == null) {
//            pool = new GcloudWorkPool();
//        }
//
//        String workerName = pool.getUnusedDomain();
//        String uri = "https://" + workerName + "." + DOMAIN;
        final Machine machine = controller.requestMachine();
        String uri = "https://" + machine.getDomainName();
        LOG.info("Sending user to " + uri);
        // if we can reach /health immediately, the machine is ready, we should send user straight there
        final boolean isDev = "true".equals(System.getProperty("devMode"));
        final boolean isReady = controller.isMachineReady(machine.getDomainName());
            if (isDev && isReady) {
                // devMode can skip cookies
                req.redirect(uri);
            } else {
                // always send user to interstitial page, so we can record our cookie before sending them along to their machine.
                req.response()
                        .putHeader("content-type", "text/html")
                        .putHeader("x-frame-options", "DENY")
                        .putHeader("Set-Cookie", COOKIE_NAME + "=" + machine.getHost() + "; Max-Age=2400; domain=" + DOMAIN + "; secure; HttpOnly")
                        .setChunked(true)
                        .setStatusCode(200)
                        // ...gross, move this to a resource file and just text replace the uri into place (store text pre-split to save IO/time)...
                        .end("<!DOCTYPE html><html><head>\n" +
                                "<style>" +
                                    "body {" +
                                        "background: #1a171a; " +
                                        "display: flex; " +
                                        "justify-content: center; " +
                                        "align-items: center; " +
                                        "height: 100vh; " +
                                        "margin 0; " +
                                        "flex-direction: column; " +
                                    " }\n" +
                                    "#box {\n" +
                                    "    margin: auto; " +
                                    "    padding: 2em; " +
                                    "    background: #fcfcfa; " +
                                    "    color: #1a171a; " +
                                    "    text-align: center; " +
                                    "    max-width: 50em; " +
                                    "    border: 0 solid rgba(26, 23, 26, 0.2); " +
                                    "    border-radius: 0.3rem; " +
                                    "    outline: 0; " +
                                    "    font-family: \"Fira Sans\", -apple-system, blinkmacsystemfont, \"Segoe UI\", \"Roboto\", \"Helvetica Neue\", arial, sans-serif; " +
                                    "}\n" +
                                    "\n" +
                                    "#box > a { " +
                                    "    color: #8b8b90; " +
                                    "    text-decoration: none; " +
                                    "    transition: color 200ms cubic-bezier(0.08,0.52,0.52,1); " +
                                    "}" +
                                "</style>\n" +
                                "</head><body>\n" +
                                "<div id=box>Preparing machine <a href=\"" + uri + "\">" + uri + "</a>.</div>\n" +
                                "<script>" +
                                    // create a small script that will ping the /health uri until it's happy, then update our message and try to navigate to running machine
                                    "var pid\n" +
                                    "var wait = 800\n" +
                                    "var request = new XMLHttpRequest();\n" +
                                    "request.onreadystatechange = function() {\n" +
                                    "    if (request.readyState === 4){\n" +
                                    "        if (request.status >= 200 && request.status < 400) {\n" +
                                    "            // we win! ...but wait 2s, to give grpc-api time to come online (or make /health smarter)\n" +
                                    "            setTimeout(goToWorker, 2000);\n" +
                                    "        } else {\n" +
                                    "            pid && clearTimeout(pid);\n" +
                                    "            pid = setTimeout(getStatus, 400);\n" +
                                    "        }\n" +
                                    "    }\n" +
                                    "};\n" +
                                    "request.onerror = function() {\n" +
                                    "    pid && clearTimeout(pid);\n" +
                                    "    pid = setTimeout(getStatus, (wait+=10));\n" +
                                    "};\n" +
                                    "function getStatus() {\n" +
                                    "    request.open(\"GET\", '" + uri + "/health' , true);\n" +
                                    "    request.send(null);\n" +
                                    "}\n" +
                                    "function goToWorker() {\n" +
                                    "    document.getElementById('box').innerHTML = 'Your machine, <a href=\"" + uri + "\">" + uri + "</a>, is ready!';\n" +
                                    "    window.location.href=\"" + uri + "\";\n" +
                                    "}\n" +
                                    "getStatus();\n" +
                                "</script>" +
                                "</body></html>");
            }
//        });
    }

    KubeManager state;
    ClusterController controller;
    private void handleKube(final RoutingContext req) {
        DhWorker worker;
        if (state == null) {
            state = new KubeManager();
        }

        try {
            // hm... we should actually send the user a response with a token that we'll
            // stream events on vert.x event bus.
            // this will let us render a "preparing your machine" loading message, while we
            // do... expensive things waiting....
            worker = state.getWorker();
        } catch (KubectlException | ApiException | InterruptedException e) {
            String unique = UUID.randomUUID().toString();
            String msg =
                    "Unknown error occurred getting a worker, please report error " + unique;
            System.err.println(msg);
            e.printStackTrace();
            vertx.runOnContext(() -> {
                req.response()
                        .setStatusCode(500)
                        .end(msg);
            });
            return;
        }
        if (worker == null) {
            // there is something that has gone terribly wrong.
            vertx.runOnContext(() -> {
                req.response()
                        .setStatusCode(503)
                        .end("The server is currently overloaded, please try again!");
            });
            return;
        }

        String uri = "https://" + worker.getDnsName();
        // ...we should probably take the /path off the source request too...

        // okay... block until this route is fully functional.
        long deadline = System.currentTimeMillis() + 180_000; // three minutes... wayyy too
        // long, really.
        while (!worker.checkReady(state.getHttpClient())) {
            // TODO: instead of this, send a response so client can give feeback while this
            // happens
            System.out.println("Waiting for " + worker.getUserName() + " ("
                    + worker.getPodName() + ") to be ready");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
            if (System.currentTimeMillis() > deadline) {
                vertx.runOnContext(() -> {
                    req.response()
                            .setStatusCode(503)
                            .end("Waited more than three minutes for " + worker.getUserName()
                                    + " to be ready");
                });
                return;
            }
        }


        vertx.runOnContext(() -> {
            req.response()
                    .putHeader("Set-Cookie",
                            "dh-user=" + worker.getUserName() + "; Secure; HttpOnly; Domain="
                                    + DOMAIN)
                    .putHeader("Location", uri)
                    .setStatusCode(302)
                    .end();
        });
    }

    public static String createIsolatedSession() throws ApiException, KubectlException {
        String userName = NameGen.newName();

        CoreV1Api api;
        try {
            api = KubeTools.getApi();
        } catch (Throwable e) {
            throw new Error("You likely have an expired auth session from kubeconfig;\n" +
                "run any kubectl command on command line to refresh manually:\n" +
                "`kubectl get pods` will suffice. The java KubeTools bail trying to call refresh() on stale auth tokens",
                e);
        }
        final V1Service[] serviceReady = {null};
        final Throwable[] failure = {null};
        // create the service in the background, since we also need to claim a pod in current thread
        new Thread("Creating service for " + userName) {
            {
                setDaemon(true);
            }

            @Override
            public void run() {
                try {
                    serviceReady[0] = prepareService(api, userName);
                } catch (ApiException e) {
                    System.err.println("Failed to load service for " + userName);
                    e.printStackTrace();
                    failure[0] = e;
                }
                synchronized (serviceReady) {
                    serviceReady.notifyAll();
                }
            }
        }.start();
        final ApiClient apiClient = api.getApiClient();
        // alright! find and claim a free pod.
        V1Pod myPod = claimPod(api, userName, 0);

        // ok, now that we have a labeled pod, wait until our services is ready so we can add a
        // route!
        while (serviceReady[0] == null) {
            synchronized (serviceReady) {
                try {
                    serviceReady.wait(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                if (failure[0] != null) {
                    throw new RuntimeException("Failed to prepare service for " + userName,
                        failure[0]);
                }
            }
        }
        // TODO: start creating the service in the background while we get a pod.
        final V1Service svc = serviceReady[0];
        if (svc == null) {
            throw new IllegalStateException("Unable to create service for " + userName);
        }
        String svcName = svc.getMetadata().getName();
        // alright!
        // last step: add a routing rule to our ingress
        final List<V1Ingress> ingresses = Kubectl.get(V1Ingress.class)
            .apiClient(apiClient)
            .namespace(NAMESPACE)
            .execute();
        // pick the ingress with the fewest rules to patch... also, prune dead weight.
        // for now... lets just edit the one, and see if it falls over
        if (ingresses.isEmpty()) {
            throw new IllegalStateException("No ingresses exist in namespace " + NAMESPACE);
        }
        final V1Ingress theOne = ingresses.get(0);
        final V1IngressSpec spec = theOne.getSpec();
        List<V1IngressRule> rules = spec.getRules();
        int ruleNum = rules.size();
        String patchy = "[" +
            "{" +
            "\"op\": \"add\", \"path\": \"/spec/rules/" + ruleNum + "\", \"value\": " +
            "{" +
            "\"host\": \"" + userName + "." + NameConstants.DOMAIN + "\"" +
            ", " +
            "\"http\": { " +
            "\"paths\": [ {" +
            "\"path\": \"/*\"" +
            " , " +
            "\"pathType\": \"ImplementationSpecific\"" +
            " , " +
            "\"backend\": {" +
            "\"service\": {" +
            "\"name\": \"" + svcName + "\"" +
            " , " +
            "\"port\": { " +
            "\"number\": " + PORT_ENVOY_CLIENT +
            " } " +
            " } " +
            " } " +
            " } ] " +
            " } " +
            "} " +
            "}" +
            "]";
        final KubectlPatch<V1Ingress> thePatch = Kubectl.patch(V1Ingress.class)
            .apiClient(apiClient)
            .namespace(NAMESPACE)
            .name(theOne.getMetadata().getName());
        thePatch.patchContent(new V1Patch(patchy))
            .patchType("json");
        final V1Ingress result = thePatch.execute();

        System.out.println("Successfully patched ingress! " + result);
        // maybe send back myPod.metadata.name as well? ...guess that _shouldn't_ be useful to
        // calling code
        return userName;
        //
        // // ok, first, looks for any pods which are advertising room for new sessions (TODO: use a
        // label / health probe to find existing pods)
        // final String conti = null;
        // final String fieldSel = null;
        // String labelSel = null;
        // final V1ServiceList result = api.listNamespacedService("default", "true", false, conti,
        // fieldSel, labelSel, 100, null, null, 10, false);
        // final List<V1Service> items = result.getItems();
        // System.out.println(items);
        //
        // labelSel = "dh-purpose=controller";
        // final String ns = "default";
        // final String pretty = "false";
        // final V1PodList pods = api.listNamespacedPod(ns, pretty, false, conti, fieldSel,
        // labelSel, 100, null, null, 10, false);
        // if (pods.getItems().isEmpty()) {
        // // this will never happen in the running cluster,
        // // as this controller will be finding it's own pod.
        // throw new IllegalStateException("No pods found with dh-purpose=controller; make sure the
        // system is healthy!");
        // }
        //
        // final String fieldManager = null;
        // String dryRun = null;// "All"; // only valid value is All
        //
        // // pick the newest pod to be our podBod
        // V1Pod podBod = pods.getItems().get(0);
        // for (V1Pod pod : pods.getItems()) {
        // final V1ObjectMeta meta = pod.getMetadata();
        // assert meta != null;
        // // don't take deleted pods
        // if (meta.getDeletionTimestamp() != null) {
        // continue;
        // }
        // if (pod != podBod) {
        // // prefer the newest pod definition
        // if (meta.getCreationTimestamp().isAfter(podBod.getMetadata().getCreationTimestamp())) {
        // podBod = pod;
        // }
        // }
        // }
        // // now, alter this pod a little.
        // podBod.getMetadata().getLabels().put("dh-purpose", "worker");
        // podBod.getMetadata().getLabels().put(NameConstants.COOKIE_NAME, userName);
        // podBod.getMetadata().setGenerateName(userName+"-");
        // podBod.getMetadata().setName(userName);
        //
        // cleanPod(podBod);
        // final V1Pod newPod = api.createNamespacedPod(ns, podBod, pretty, dryRun, fieldManager);
        // System.out.println(newPod);
        //
        //
        // // dirtiness... good enough for now.
        // new Thread("LogFor" + userName) {
        // final KubectlLog log = Kubectl.log();
        // final KubectlGet<V1Pod> podGet = Kubectl.get(V1Pod.class);
        // {
        // podGet.apiClient(api.getApiClient());
        // final KubectlGet<V1Pod>.KubectlGetSingle podGetSingle =
        // podGet.namespace(ns).name(userName);
        // try {
        //
        // final V1Pod foundPod = podGetSingle.execute();
        // System.out.println("The pod: " + (foundPod == null ? "is null" : "has status: " +
        // foundPod.getStatus()));
        // //System.out.println(foundPod.stream().map(new Function<V1Pod, String>(){ public String
        // apply(V1Pod p) { return p.getMetadata().getNamespace() + " : " +
        // p.getMetadata().getName();}}).collect(Collectors.joining("\n")));
        // } catch (KubectlException e) {
        // e.printStackTrace();
        // }
        //
        // log.apiClient(api.getApiClient());
        // log.name(userName);
        // log.namespace(newPod.getMetadata().getNamespace());
        // // inside the worker, grpc-api is where the interesting logs are
        // // (though, we should probably tail the controller health check logs instead, they are
        // boring atm)
        // log.container("grpc-api");
        // setDaemon(true);
        // }
        //
        // @Override
        // public void run() {
        // final InputStream in;
        // try {
        // in = log.execute();
        // } catch (KubectlException e) {
        // System.err.println("Unable to stream logs for " + userName);
        // e.printStackTrace();
        // return;
        // }
        // try {
        // IOUtils.copy(in, System.out);
        // } catch (IOException e) {
        // System.err.println("Error streaming logs from " + userName);
        // e.printStackTrace();
        // }
        // }
        // }.start();
        //
        // // WIP: DO NOT COMMIT!
        // try {
        // Thread.sleep(60_000);
        // } catch (InterruptedException e) {
        // e.printStackTrace();
        // }
    }

    private static V1Service prepareService(final CoreV1Api api, final String userName)
        throws ApiException {
        final V1ObjectMeta metadata = new V1ObjectMeta();
        final Map<String, String> annos = new LinkedHashMap<>();
        annos.put(ANNO_BACKEND_CONFIG, BACKEND_ENVOY);
        annos.put(ANNO_APP_PROTOCOLS, PROTOCOL_HTTP2);
        annos.put(ANNO_NEG, NEG_INGRESS);
        metadata.setAnnotations(annos);
        final LinkedHashMap<String, String> labels = new LinkedHashMap<>();
        labels.put(LABEL_USER, userName);
        metadata.setLabels(labels);
        metadata.setName(userName);
        metadata.setNamespace(NAMESPACE);

        final V1ServiceSpec spec = new V1ServiceSpec();
        spec.setType("NodePort");
        spec.setExternalTrafficPolicy("Cluster");

        final List<V1ServicePort> portList = new ArrayList<>();
        final V1ServicePort clientPort = new V1ServicePort();
        clientPort.setName(PORT_ENVOY_CLIENT_NAME);
        clientPort.setPort(PORT_ENVOY_CLIENT);
        clientPort.setTargetPort(new IntOrString(PORT_ENVOY_CLIENT));
        clientPort.setProtocol("TCP");
        portList.add(clientPort);
        spec.setPorts(portList);

        final Map<String, String> selector = new LinkedHashMap<>();
        selector.put(LABEL_USER, userName);
        spec.setSelector(selector);

        V1Service newSvc = new V1Service();
        newSvc.setApiVersion("v1");
        newSvc.setKind("Service");
        newSvc.setMetadata(metadata);
        newSvc.setSpec(spec);

        final V1Service createdSvc =
            api.createNamespacedService(NAMESPACE, newSvc, null, null, null);
        return createdSvc;
    }

    private static V1Pod claimPod(final CoreV1Api api, final String podName, final int tries)
        throws ApiException, KubectlException {
        V1PodList freePods = findFreePods(api);
        for (V1Pod pod : freePods.getItems().stream().sorted((a, b) -> {
            // when we go to pick a pod, lets prefer Running over !Running, then oldest over newest
            if ("Running".equals(a.getStatus().getPhase())) {
                if (!"Running".equals(b.getStatus().getPhase())) {
                    return -1;
                }
            } else if ("Running".equals(b.getStatus().getPhase())) {
                return 1;
            }
            return a.getMetadata().getCreationTimestamp()
                .compareTo(b.getMetadata().getCreationTimestamp());
        }).collect(Collectors.toList())) {
            final String user = pod.getMetadata().getLabels().get(NameConstants.LABEL_USER);
            if (user == null) {
                // try to claim it!
                if (tryClaim(api.getApiClient(), pod, podName)) {
                    return pod;
                }
            } else if (user.equals(podName)) {
                return pod;
            } else {
                // somebody else got it first, keep trying.
                System.out.println("Someone else (" + user + ") won " + pod.getMetadata().getName()
                    + " over us (" +
                    podName + ")");
            }
        }
        if (tries > 5) {
            throw new IllegalStateException("Cannot find a free pod for " + podName);
        }
        return claimPod(api, podName, tries + 1);
    }

    private static boolean tryClaim(final ApiClient apiClient, final V1Pod pod,
        final String userName) throws KubectlException {
        // attempt to patch the pod labels.
        final V1Pod result;
        final String realName = pod.getMetadata().getName();
        try {
            result = Kubectl.label(V1Pod.class)
                .apiClient(apiClient)
                .namespace(pod.getMetadata().getNamespace())
                .name(pod.getMetadata().getName())
                .addLabel(LABEL_USER, userName)
                .addLabel(LABEL_PURPOSE, PURPOSE_WORKER)
                .execute();
        } catch (KubectlException e) {
            e.printStackTrace();
            return false;
        }

        // hokay! we got back a result, assume we may _not_ have won a race!
        final V1Pod check = Kubectl.get(V1Pod.class)
            .apiClient(apiClient)
            .namespace(NAMESPACE)
            .name(realName)
            .execute();

        final String realLabel = check.getMetadata().getLabels().get(NameConstants.LABEL_USER);
        if (userName.equals(realLabel)) {
            System.out
                .println(NameGen.getMyName() + " won pod " + realName + " for user " + userName);
            return true;
        }
        System.out.println(NameGen.getMyName() + " lost pod " + realName + " for user " + userName);
        // This weird log is here so we can detect if race conditions actually happen, or if we'll
        // know we lost immediately
        System.out.println(
            "Pre-check label: " + result.getMetadata().getLabels().get(NameConstants.LABEL_USER));
        return false;
    }

    private static void scaleUp(final ApiClient apiClient) {
        final KubectlGet<V1Deployment>.KubectlGetSingle deployment =
            Kubectl.get(V1Deployment.class).name(NAME_DEPLOYMENT);
        deployment.namespace(NAMESPACE).apiClient(apiClient);
        final V1Deployment deploy;
        try {
            deploy = deployment.execute();
            // now, lets try to update this deploy...
            assert deploy.getSpec() != null;
            Integer replicas = deploy.getSpec().getReplicas();
            assert replicas != null;

            // HM... we should count how many pods there are...
            final ListOptions countOpts = new ListOptions();
            countOpts.setLimit(replicas + 10);
            final List<V1Pod> podList = Kubectl.get(V1Pod.class)
                .apiClient(apiClient)
                .namespace(NAMESPACE)
                .options(countOpts)
                .execute();
            System.out
                .println("There are " + podList.size() + " pods for " + replicas + " replicas");
            if (podList.size() < replicas) {
                return;
            }

            if (replicas < 3) {
                replicas = replicas + 2;
            } else if (replicas < 7) {
                replicas = replicas + 4;
            } else if (replicas < 15) {
                replicas = replicas + 8;
            } else if (replicas < 50) {
                replicas = replicas + 16;
            }

            // Before we actually trigger the scale up,
            // we need to do a little rate limiting... don't want to go 1->100 in 4.3 seconds now!
            if (shouldSkipScaleup(apiClient, podList)) {
                System.out.println(
                    "Too many scale ups in too short of a period; skipping scale up request");
                return;
            }

            final V1Deployment scale = Kubectl.scale(V1Deployment.class)
                .apiClient(apiClient)
                .namespace(NAMESPACE)
                .name(NAME_DEPLOYMENT)
                .skipDiscovery()
                .replicas(replicas)
                .execute();

            assert scale.getSpec() != null;
            System.out.println("Scaled replicas to " + scale.getSpec().getReplicas());

        } catch (KubectlException e) {
            System.err.println("Error trying to read deployment " + deployment);
            e.printStackTrace();
        }
    }

    private static boolean shouldSkipScaleup(final ApiClient apiClient, final List<V1Pod> podList) {
        // If there is a fair amount of not-yet-running pods,
        // we should instead wait a little while for those pods to come online
        for (V1Pod pod : podList) {
            final Map<String, String> podLabels = pod.getMetadata().getLabels();
            final String phase = pod.getStatus().getPhase();
            System.out.println("Pod " + pod.getMetadata().getName() + " has labels " + podLabels);
            System.out.println("Pod " + pod.getMetadata().getName() + " has phase " + phase);
        }

        // for testing, for now...
        return podList.size() > 3;
    }

    private static V1PodList findFreePods(final CoreV1Api api) throws ApiException {
        final String fieldSel = "status.phase!=Failed";
        final String labelSel = "!" + LABEL_USER;
        V1PodList result = api.listNamespacedPod(NAMESPACE, null, false, null, fieldSel, labelSel,
            5, null, null, 10, false);
        long scaleWait = 2_000;
        if (result.getItems().isEmpty()) {
            // no free pods :'( scale up!
            scaleUp(api.getApiClient());
            while (result.getItems().isEmpty()) {
                try {
                    Thread.sleep(scaleWait);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                scaleWait = scaleWait * 2;
                // we recurse here, thus the tiny sleep, above
                result = findFreePods(api);
            }
        }
        return result;
    }

    private static void ensureCertificates(final List<String> extraArgs)
        throws IOException, InterruptedException {
        // actually generate any necessary certificates
        CertFactory.main(extraArgs.toArray(new String[0]));
    }

    private static ExecutorService executor = Executors.newFixedThreadPool(6);

    private static void startGroovySession(final HttpServerRequest request)
        throws IOException, ApiException, KubectlException {
        startLanguageSession("Groovy", request);
    }

    private static void startPythonSession(final HttpServerRequest request)
        throws IOException, ApiException, KubectlException {
        startLanguageSession("Python", request);
    }

    private static void startLanguageSession(String lang, final HttpServerRequest request) {
        // get off the vert.x event queue
        executor.submit(() -> {
            final String url;
            try {
                url = getOrCreateLanguageSession(lang, request);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
            // reenter the vertx context to deliver the session
            Vertx.currentContext().runOnContext(() -> {
                request.response().putHeader("location", url)
                    .setStatusCode(302)
                    .end();
            });
            return url;
        });
    }

    private static String getOrCreateLanguageSession(String lang, final HttpServerRequest request)
        throws IOException, ApiException, KubectlException, InterruptedException {
        // alright, start up a groovy worker, and send the user to it via redirect
        System.out.println("Starting a " + lang + " session for " + dumpSession(request));
        String url = "https://deephaven.io";

        // ok, tell kube cluster to spin up a new host!

        CoreV1Api api = KubeTools.getApi();
        // ok, first, looks for any pods which are advertising room for new sessions (TODO: use a
        // label / health probe to find existing pods)
        V1Pod chosenOne = findChosenOne(api, request);
        long backoff = 100;

        if (chosenOne == null) {
            ensureNamespace(api);
            // if nothing is found, create a new pod to connect to
            int machineCnt = countMachines(api);
            // now, we should really wait on this to scale up!
            int newMachines = doScaleUp(api, request);
            int nextCnt;
            while (machineCnt < newMachines) {
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
        url = "http://" + chosenOne.getSpec().getHostname() + ":8765/ide/"; // this is almost
                                                                            // certainly wrong.

        // for (V1Pod item : list.getItems()) {
        // System.out.println(item.getMetadata().getName());
        // }
        return url;
    }

    private static V1Pod findChosenOne(final CoreV1Api api, final HttpServerRequest request)
        throws ApiException {

        V1PodList list = api.listNamespacedPod(DH_NAMESPACE, null, null, null,
            "metadata.name=dh-local", "run=dh-local", null, null, null, 10, false);
        findTheOne: for (V1Pod item : list.getItems()) {
            // check if this pod has room for us.
            final V1PodStatus status = item.getStatus();
            if (status == null) {
                continue;
            }
            final OffsetDateTime started = status.getStartTime();
            // TODO: decide how long to keep pods around before we stop scheduling to them and let
            // them shut down.
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
            // TODO: something smart, where we actually query the state of the pod before sending it
            // work.
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
            String indent = "    ";
            StringBuilder patchLabel = new StringBuilder("metadata:\n" +
                "  labels:\n");
            for (Map.Entry<String, String> label : labels.entrySet()) {
                patchLabel.append(indent).append(label.getKey()).append(": \"")
                    .append(label.getValue()).append("\"\n");
            }

            final V1Patch labelPatch = new V1Patch(patchLabel.toString());
            final V1Pod patchResult = api.patchNamespacedPod(metadata.getName(),
                metadata.getNamespace(), labelPatch, null, null, null, null);
            final String actual = patchResult.getMetadata().getLabels().get(DH_POD_KEY);
            if (!key.equals(actual)) {
                System.out
                    .println("We lost a contentious grab for pod " + item.getMetadata().getName());
                continue;
            }
            // we were able to set the label to our unique id, it's ours!
            return item;
        }
        return null;
    }

    private static int countMachines(final CoreV1Api api) throws ApiException {
        final V1PodList results = api.listNamespacedPod(DH_NAMESPACE, null, null, null,
            "metadata.name=dh-local", null, 10_000, null, null, 2, null);
        final List<V1Pod> items = results.getItems();
        return items.size();
    }

    private static V1Pod newPodSpec(final CoreV1Api api, final HttpServerRequest request)
        throws KubectlException {
        final V1Pod pod = new V1Pod();
        pod.setApiVersion("v1");
        return pod;
    }

    private static int doScaleUp(final CoreV1Api api, final HttpServerRequest request)
        throws KubectlException, ApiException, IOException {
        // ensureNamespace(api);
        AppsV1Api appsApi = new AppsV1Api(api.getApiClient());
        KubectlGet<V1Deployment> getDeploy = Kubectl.get(V1Deployment.class)
            .namespace(DH_NAMESPACE)
            .apiListTypeClass(V1DeploymentList.class);
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
            final V1Deployment fromYaml = Yaml.loadAs(new File(
                "/dh/ws0/deephaven-core/demo/src/main/resources/io/deephaven/demo/dh-localhost.yaml"),
                V1Deployment.class);
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
            // "apiVersion: apps/v1\n" +
            // "type: Deployment\n" +
            "spec:\n" +
                "  replicas: " + newReps;

        final V1Patch patchUpdate = new V1Patch(patchContent);

        // patchDeploy
        // .namespace(DH_NAMESPACE)
        // .name(DH_DEPLOY_NAME)
        // .patchContent(patchUpdate);
        // final V1Deployment result = patchDeploy.execute();

        final PatchUtils.PatchCallFunc a;
        final String b;
        final ApiClient c;
        V1Deployment res = PatchUtils.patch(V1Deployment.class,
            () -> appsApi.patchNamespacedDeploymentCall(DH_DEPLOY_NAME, DH_NAMESPACE,
                patchUpdate, null, null, "kubectl", null, null),
            V1Patch.PATCH_FORMAT_APPLY_YAML, appsApi.getApiClient());
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
        return api.listNamespace(null, null, null, "metadata.name=" + DH_NAMESPACE, null, null,
            null, null, 2, null);
    }

    private static String dumpSession(final HttpServerRequest request) {
        return request.absoluteURI() + " with headers:\n" +
            request.headers().entries().stream().map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("\n"));
    }
}
