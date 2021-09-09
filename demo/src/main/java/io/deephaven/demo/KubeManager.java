package io.deephaven.demo;

import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.extended.kubectl.Kubectl;
import io.kubernetes.client.extended.kubectl.KubectlGet;
import io.kubernetes.client.extended.kubectl.KubectlPatch;
import io.kubernetes.client.extended.kubectl.exception.KubectlException;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.generic.options.ListOptions;
import io.smallrye.common.constraint.NotNull;
import io.smallrye.common.constraint.Nullable;
import io.vertx.mutiny.core.Vertx;
import okhttp3.OkHttpClient;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.deephaven.demo.NameConstants.*;

/**
 * KubeState:
 * <p>
 * <p> A service for tracking the current state of our kubernetes cluster.
 * <p> Rather than make every request for information request lists of things,
 * <p> we can just keep low-ttl, manually-invalidatable caches.
 * <p>
 * <p> For now, we're just going to do network requests everywhere, but done through this class
 * <p> so we have a single place to manage those caching details.
 */
public class KubeManager {

    private final Map<String, DhWorker> workers;
    private final CoreV1Api api;
    private final ApiClient apiClient;
    private final AtomicInteger ingressCnt = new AtomicInteger();
    /**
     * Instead of Thread.sleep(...), we use <pre>synchronized(stateRequestLock) { stateRequestLock.wait(...); }</pre>
     * <p>
     * <p> This allows us to <pre>synchronized(stateRequestLock) { stateRequestLock.notify(); }</pre> to wake up the get-state-thread.
     * <p> Once you have .notify() this lock, you should then wait for a re-sync to finish:
     * <pre>synchronized(stateResponseLock) { stateResponseLock.wait(...) }</pre>
     * <p> The response lock will be notified whenever state is re-read, or if re-reading is skipped due to too many requests.
     *
     */
    private final Object stateRequestLock = new Object();
    private final Object stateResponseLock = new Object();
    private volatile KubeState state;
    private final OkHttpClient httpClient;

    public KubeManager() {
        httpClient = new OkHttpClient.Builder().build();
        workers = new HashMap<>();
        api = KubeTools.getApi();
        apiClient = api.getApiClient();
        if (checkIfLeader()) {
            // only the elected master should handle monitoring the state of whole cluster.
            // we really, really don't want races going on here.
            new Thread("KubeState Monitor") {

                @Override
                public void run() {
                    System.out.println("Leader node starting KubeState monitoring loop");
                    while (true) {

                        // ok, lets collect up all of our ingresses, services and pods into a new state.
                        try {
                            KubeState oldState;
                            synchronized (stateResponseLock) {
                                oldState = state;
                                state = computeState();
                                // wake up anyone who might be waiting on fresh state.
                                stateResponseLock.notifyAll();
                            }
                            processStateChanges(oldState, state);
                            synchronized (stateRequestLock) {
                                // wait up to 15s before we naturally re-check state.
                                stateRequestLock.wait(15_000);
                            }
                        } catch (InterruptedException e) {
                            System.out.println("KubeState Monitor interrupted, returning");
                            return;
                        }
                    }
                }

                {
                    setDaemon(true);
                    setUncaughtExceptionHandler((t, e) -> {
                        System.err.println("Unhandled exception in KubeState Monitor");
                        e.printStackTrace();
                    });
                }

            }.start();
        } else {
            // we are not the leader.
            // TODO: consider loading KubeState by pinging leader on a url like /state? ...seems like a security risk.
        }
    }

    /**
     * Called whenever we compute a new {@link KubeState} with the previous, nullable KubeState.
     * <p>
     * <p> This is where we will detect when a worker is unhealthy or expired, and delete it.
     * <p>
     * @param oldState our this.state field value before we called {@link #computeState()}
     * @param state our this.state field value after we called {@link #computeState()}
     */
    private void processStateChanges(final @Nullable KubeState oldState, @NotNull final KubeState state) {
        // check that all the sessions are healthy;
        // cleanup anything that's broken / in a FAILED state.
        Map<String, ConcurrentLinkedQueue<String>> ingressPatches = new ConcurrentHashMap<>();
        state.getSessions().parallel().forEach(session -> {
            String userName = session.getUserName();
            if (isUnhealthy(session)) {
                System.out.println("[WARN] Removing no-longer-healthy username " + userName +" : " + session);
                final DhWorker removed = workers.get(userName);
                if (removed != null) {
                    // hm... we should be batching the ingress patch calls into one here...
                    cleanupWorker(removed, ingressPatches);
                }
            } else {
                DhWorker worker = workers.get(userName);
                if (worker == null) {
                    if (session.isComplete()) {
                        worker = session.asWorker();
                        workers.put(userName, worker);
                    } else {
                        System.out.println("Incomplete session for " + userName +" :\n" + session);
                    }
                } else {
                    // hm, perhaps validate that the worker is correct, and replace it if it's expired?
                    // we maintain state on our thin DhWorker object
                }
                if (worker != null && !worker.isReady()) {
                    // if worker is not marked ready, we may want to update worker state
                }
            }
        });

        if (oldState == null) {
            return;
        }
        oldState.getSessions().parallel().forEach(oldSession -> {
            String uname = oldSession.getUserName();
            if (state.hasSession(oldSession.getUserName())) {
                // hm... we could diff session values to notify on change... but we have nothing needing such a callback atm.
            } else {
                // current state no longer has this entry... delete our worker!
                System.out.println("[INFO] Removing no-longer-present username " + uname);
                final DhWorker removed = workers.remove(uname);
                cleanupWorker(removed, ingressPatches);
            }
        });

        if (!ingressPatches.isEmpty()) {
            System.out.println("[INFO] Patching " + ingressPatches.size() + " ingresses");
            ingressPatches.entrySet().parallelStream().forEach(item -> {
                final KubectlPatch<V1Ingress> patchCall = Kubectl.patch(V1Ingress.class)
                        .apiClient(apiClient)
                        .namespace(DH_NAMESPACE)
                        .name(item.getKey());
                final String body = item.getValue().stream().collect(Collectors.joining(",\n"));
                final V1Patch patch = new V1Patch(
                        "" +
                        body +
                        ""
                );
                patchCall.patchContent(patch);

            });
        }
    }

    private boolean isUnhealthy(final KubeSession session) {

        // check pod
        if (session.getPod() == null) {
            // we don't consider a not-yet-initialized session as unhealthy.
            return false;
        }
        final V1PodStatus podStatus = session.getPod().getStatus();
        if (podStatus == null) {
            return false;
        }
        if ("Failed".equals(podStatus.getPhase())) {
            return true;
        }

        // check service
        if (session.getService() == null) {
            // we don't consider a not-yet-initialized session as unhealthy.
            return false;
        }
        final V1ServiceStatus serviceStatus = session.getService().getStatus();
        if (serviceStatus == null) {
            return false;
        }
        if (serviceStatus.getConditions() != null) {
            for (V1Condition condition : serviceStatus.getConditions()) {
                if ("False".equals(condition.getStatus())) {
                    return true;
                }
            }
        }
        // serviceStatus.getLoadBalancer().getIngress()


        // we should perhaps institute a time-limit as well.
        if (session.getPod().getMetadata().getCreationTimestamp().isBefore(getPodMaxLife())) {
            // this pod is too old!
            // ...lets tell the user their session is dead, and kill it
        }

        return false;
    }

    private OffsetDateTime getPodMaxLife() {
        // TODO: make this configurable
        return OffsetDateTime.now().minus(50, ChronoUnit.MINUTES);
    }

    private void cleanupWorker(final DhWorker removed, final Map<String, ConcurrentLinkedQueue<String>> ingressPatches) {
        if (removed == null) {
            // tolerate nulls.
            return;
        }
        removed.setDestroyed(true);
        // delete kube resources on background thread, prepare ingress patches
        Vertx.currentContext().owner().setTimer(0, later-> {
            if (removed.getPodName() != null) {
                // delete the pod
            }
            if (removed.getServiceName() != null) {
                // delete the service
            }
        });
        if (removed.getIngressName() != null) {

        }
    }

    /**
     * Gets or computes/blocks until the KubeState is fully read.
     * <p>
     * <p> This method will only call {@link #computeState()} if the state is not yet initialized when invoked.
     * <p> If the get-state-thread is already running, we'll use a double-checked lock to avoid spurious recomputes.
     * <p>
     * @return a KubeState, as soon as one is ready to read.
     */
    public KubeState getState() {
        if (state == null) {
            synchronized (stateResponseLock) {
                if (state == null) {
                    state = computeState();
                    stateResponseLock.notifyAll();
                }
            }
        }
        return state;
    }

    private KubeState computeState() {

        KubeState state = new KubeState();
        final List<Throwable> failures = state.computeNow(this);
        if (failures != null && !failures.isEmpty()) {
            // kube logs split up by line, so ugly toStringing the exceptions here can help when searching [ERROR]
            System.err.println("[ERROR] failures computing KubeState: " + failures);
            for (Throwable failure : failures) {
                failure.printStackTrace(System.err);
            }
            // for now, we'll keep running... in the future, we may want to consider self-destruction here.
        }
        return state;
    }

    private boolean checkIfLeader() {
        // check if we are the leader node.

        // on a real system, every controller will have a MY_POD_NAME env var set, so we can check ourself for pod labels
        String myPod = System.getenv("MY_POD_NAME");
        if (myPod == null) {
            // if the var is not set, then we are running localhost
            return true;
        }

        // check our own pod for the a `dh-controller: master` label.
        final V1Pod pod;
        try {
            pod = Kubectl.get(V1Pod.class)
                    .apiClient(apiClient)
                    .namespace(DH_NAMESPACE)
                    .name(myPod)
                    .execute();
        } catch (KubectlException e) {
            System.err.println("Unable to get metadata for pod " + myPod);
            e.printStackTrace();
            return false;
        }
        final Map<String, String> labels = pod.getMetadata().getLabels();
        if (labels == null) {
            return false;
        }
        String controllerType = pod.getMetadata().getLabels().get(LABEL_PURPOSE);
        if (PURPOSE_CONTROLLER.equals(controllerType)) {
            return true;
        }
        // We are a worker, and should not be handling any leader-node responsibilities
        return false;
    }

    public List<V1Service> getWorkerServices() throws ApiException {
        String cntu = null;
        final String fieldSel = ""; // we want the controller to see ALL services. "status.phase!=Failed";
        final String labelSel = LABEL_PURPOSE + "=" + PURPOSE_WORKER;
        // we can page these, and we might as well do decent-sized chunks.
        final Integer limit = 400;
        V1ServiceList result = api.listNamespacedService(DH_NAMESPACE, "False", false, cntu, fieldSel, labelSel, limit, null, null, 5, false);
        List<V1Service> all = new ArrayList<>(result.getItems());
        while (result.getMetadata() != null && result.getMetadata().getContinue() != null) {
            result = api.listNamespacedService(DH_NAMESPACE, "False", false, result.getMetadata().getContinue(), fieldSel, labelSel, limit, null, null, 5, false);
            all.addAll(result.getItems());
        }
        return all;
    }

    public List<V1Ingress> getWorkerIngress() throws KubectlException {
        final ListOptions listOptions = new ListOptions();
        // it's a big pain right now to get _continue token out of kubernetes api response.
        // we can dig into the .execute() method below to get "inspiration" on how to read the continue token and page results.
        listOptions.setLimit(50_000);
        List<V1Ingress> ingresses = Kubectl.get(V1Ingress.class)
                .apiClient(apiClient)
                .namespace(DH_NAMESPACE)
                .options(listOptions)
                .execute();

        return ingresses;
    }

    public List<V1Pod> getWorkerPods() throws ApiException {
        String cntu = null;
        final String fieldSel = ""; // we want the controller to see ALL pods, not just: "status.phase!=Failed";
        final String labelSel = LABEL_PURPOSE + "=" + PURPOSE_WORKER;
        // we can page these, and we might as well do decent-sized chunks.
        final Integer limit = 400;
        V1PodList result = api.listNamespacedPod(DH_NAMESPACE, "False", false, cntu, fieldSel, labelSel, limit, null, null, 5, false);
        List<V1Pod> all = new ArrayList<>(result.getItems());
        while (result.getMetadata() != null && result.getMetadata().getContinue() != null) {
            result = api.listNamespacedPod(DH_NAMESPACE, "False", false, result.getMetadata().getContinue(), fieldSel, labelSel, limit, null, null, 5, false);
            all.addAll(result.getItems());
        }
        return all;
    }

    public List<V1Pod> getAvailableWorkers() {
        final List<V1Pod> list = null;
        return list;
    }

    public boolean hasValidRoute(final String uname) {
        final DhWorker worker = workers.get(uname);
        if (worker == null) {
            return false;
        }
        if (worker.checkReady(httpClient)) {
            worker.setInUse(true);
            return true;
        }
        System.out.println("[WARN] Found not-ready worker " + worker);
        return false;
    }2552598

    public DhWorker createWorker() throws KubectlException, ApiException, InterruptedException {
        DhWorker result = null;
        long start = System.nanoTime();
        try {
            result = doCreateWorker();
            return result;
        } finally {
            String msg;
            if (result == null) {
                msg = "[FAILURE] Failed to find worker";
            } else {
                msg = "[SUCCESS] Assigned worker " + result.getUserName();
            }
            long timeUsed = System.nanoTime() - start;
            if (timeUsed < 1_000_000) {
                System.out.println(msg + " in " + timeUsed + " nanoseconds");
            } else if (timeUsed < 2_000_000_000) {
                System.out.println(msg + " in " + TimeUnit.NANOSECONDS.toMillis(timeUsed) + " milliseconds");
            } else {
                System.out.println(msg + " in " + TimeUnit.NANOSECONDS.toSeconds(timeUsed) + " seconds");
            }
        }
    }
    private DhWorker doCreateWorker() throws KubectlException, ApiException, InterruptedException {
        // First, ping the check-state-thread, to get it reading data
        final KubeState origState = state;
        refreshWorkers();
        // Next, try to pull a ready-to-go worker off the queue.
        DhWorker readyWorker = findReadyWorker();
        if (readyWorker != null) {
            return readyWorker;
        }

        boolean sawChanges = false;
        // No workers that we know about are ready. Refresh the list.
        if (waitForRefreshedWorkerList(origState)) {
            // if the refresh found new workers, try to claim one.
            sawChanges = true;
            readyWorker = findReadyWorker();
            if (readyWorker != null) {
                return readyWorker;
            }
        }

        // ok... if there are workers who are still spinning up, we should wait on one of them.
        // before we pick a pending machine though, lets wait a little longer for refreshed sessions metadata.
        if (!sawChanges) {
            int waits = 3;
            while (waits --> 0) {
                if (waitForRefreshedWorkerList(origState)) {
                    break;
                }
            }
        }
        // pick a pending pod, preferring one that is running
        DhWorker pending = findPendingWorker();
        if (pending != null) {
            // ok, there is a pending machine that should be ready soon. Send id to client so they can wait
            // (that is, poll for https://user-name.site.com/health until it's ready)
            maybeScaleUp();
            return pending;
        }

        // ok, there's literally nothing pending. Must scale up.
        scaleUp();
        refreshWorkers();
        waitForRefreshedWorkerList(origState);
        readyWorker = findReadyWorker();
        if (readyWorker != null) {
            return readyWorker;
        }
        pending = findPendingWorker();
        if (pending != null) {
            return pending;
        }
        System.err.println("NO WORKERS AVAILABLE AFTER SCALE UP; SOMETHING HAS GONE TERRIBLY WRONG!");
        return null;
    }

    private DhWorker findPendingWorker() {
        DhWorker best = null;
        synchronized (workers) {
            for (DhWorker worker : workers.values()) {
                if (!worker.isInUse()) {
                    if (best == null) {
                        best = worker;
                    } else if (best.getCreatedTime() > worker.getCreatedTime()) {
                        best = worker;
                    }
                    if (worker.isReady()) {
                        worker.setInUse(true);
                        return worker;
                    }
                }
            }
            if (best != null) {
                best.setInUse(true);
            }
        }
        return best;

    }
    private DhWorker findReadyWorker() throws KubectlException, ApiException {
        Set<DhWorker> notReadyYet = new HashSet<>();
        synchronized (workers) {
            for (DhWorker worker : workers.values()) {
                if (!worker.isInUse()) {
                    if (!worker.isReady()) {
                        notReadyYet.add(worker);
                        continue;
                    }
                    worker.setInUse(true);
                    maybeScaleUp();
                    return worker;
                }
            }
        }

        // If we have no workers who are ready to go,
        // then we either need to scale up, or block until a pending worker is ready.
        if (!notReadyYet.isEmpty()) {
            // we have some machines that are not in use, and not marked ready yet.
            final DhWorker readyWorker = claimIfReady(notReadyYet);
            if (readyWorker != null) {
                maybeScaleUp();
                return readyWorker;
            }
        }
        return null;
    }

    private void refreshWorkers() {
        synchronized (stateRequestLock) {
            stateRequestLock.notifyAll();
        }
    }

    private boolean waitForRefreshedWorkerList(final KubeState origState) throws InterruptedException {
        if (origState != state) {
            // request already completed while calling code was busy
            return true;
        }
        // pull in all the pods
        synchronized (stateResponseLock) {
            // wait up to 4 seconds for a response... we try twice as well, so this should be enough latency, I hope!
            stateResponseLock.wait(4_000);
        }
        final KubeState newState = state;
        return origState != newState;
    }

    private DhWorker claimIfReady(final Set<DhWorker> notReadyYet) {
        // for each of these workers, refresh the state of their pod
        final Optional<DhWorker> winner = notReadyYet.parallelStream().filter(worker -> {
            // check if we can reach the machine over network
            if (worker.checkReady(httpClient)) {
                // attempt to claim the worker
                synchronized (workers) {
                    // TODO: claim by updating a label and checking for race conditions... outside the synchro block.
                    if (!worker.isInUse()) {
                        worker.setInUse(true);
                        return true;
                    }
                }
            }
            return false;
        }
        ).findFirst();

        return winner.orElse(null);
    }

    private void maybeScaleUp() throws KubectlException, ApiException {
        float numUsed = 0, numAvail = 0;
        synchronized (workers) {
            for (DhWorker value : workers.values()) {
                if (value.isInUse()) {
                    numUsed++;
                } else {
                    numAvail++;
                }
            }
            if (numAvail < 2) {
                // whenever we have less than 2, always scale up!
                scaleUp();
            }
            if (numAvail / numUsed < 0.25) {
                scaleUp();
            }
        }

    }

    private void scaleUp() throws KubectlException, ApiException {
        int current = workers.size(), batchSize;
        if (current < 10) {
            batchSize = 4;
        } else if (current < 50) {
            batchSize = 10;
        } else {
            batchSize = 25;
        }
        // Now, scale up the deployment size, add services and an Ingress.
        V1Deployment deployment = getDeployment();
        Integer replicas = deployment.getSpec().getReplicas();
        int desired = (replicas == null ? 0 : replicas) + batchSize;

        final V1Deployment scale = Kubectl.scale(V1Deployment.class)
                .apiClient(apiClient)
                .namespace(NAMESPACE)
                .name(NAME_DEPLOYMENT)
                .skipDiscovery()
                .replicas(desired)
                .execute();

        assert scale.getSpec() != null;
        System.out.println("Scaled replicas to " + scale.getSpec().getReplicas());

        // ok, deployments scaled up. Lets assign names, setup services, create ingress.
        // this might technically be happening in a race condition, so we'll be extra careful.
        final String fieldSel = "status.phase!=Failed";
        final String labelSel = "!" + LABEL_USER;
        String ingressName = newIngressName();

        V1PodList result = api.listNamespacedPod(NAMESPACE, null, false, null, fieldSel, labelSel, 5, null, null, 10, false);
        List<DhWorker> needService = new ArrayList<>();
        result.getItems().parallelStream().forEach(pod -> {
            // add the dh.user label, once we confirm we won the label, add to our needService list.
            DhWorker worker = null;
            try {
                worker = setupPod(pod, ingressName);
            } catch (KubectlException e) {
                e.printStackTrace();
            }
            if (worker != null) {
                needService.add(worker);
            }
        });
        if (!needService.isEmpty()) {
            // we have created at least one pod, create services and an Ingress for it.
            List<String> needsIngress = new ArrayList<>();
            needService.parallelStream().forEach(worker -> {
                try {
                    createService(worker.getUserName());
                    needsIngress.add(worker.getUserName());
                } catch (ApiException e) {
                    System.err.println("Unable to create service for worker " + worker.getUserName());
                    e.printStackTrace();
                    // TODO: schedule the pod for deletion...
                }
            });
            createIngress(ingressName, needService.stream()
                .map(DhWorker::getUserName).collect(Collectors.toList()));
        }

    }

    String newIngressName() {
        return DH_INGRESS_NAME + "-" + NameGen.getMyName() + "-" + ingressCnt.incrementAndGet();
    }

    public V1Ingress createIngress(final String ingressName, final Iterable<String> needService) throws KubectlException {
        // alright! create an all new Ingress that maps to all of the services we have created.
        // rather than hardcode all the values that helm chart may have overridden,
        // we'll instead load up the one-deployed ingress named dh-ingress
        final V1Ingress ingress = Kubectl.get(V1Ingress.class)
                .apiClient(apiClient)
                .namespace(DH_NAMESPACE)
                .name(DH_INGRESS_NAME)
                .execute();

        V1Ingress newIngress = new V1Ingress();
        copyBasics(ingressName, ingress, newIngress);
        // now, add some rules to the spec
        final V1IngressSpec spec = new V1IngressSpec();
        final List<V1IngressRule> rules = new ArrayList<>();
        for (String worker : needService) {
            V1IngressRule rule = new V1IngressRuleBuilder()
                    .withHost(worker + "." + DOMAIN)
                    .withNewHttp()
                        .addNewPath()
                            .withPath("/*")
                            .withPathType("ImplementationSpecific")
                            .withNewBackend()
                                .withNewService()
                                    .withName(worker)
                                    .withNewPort()
                                        .withNumber(PORT_ENVOY_CLIENT)
                                    .endPort()
                                .endService()
                            .endBackend()
                        .endPath()
                    .endHttp()
                    .build();

            rules.add(rule);
        }

        spec.setRules(rules);
        newIngress.setSpec(spec);


        // alright! create the ingress!
        final V1Ingress result = Kubectl.create(V1Ingress.class)
                .apiClient(apiClient)
                .namespace(DH_NAMESPACE)
                .name(newIngress.getMetadata().getName())
                .resource(newIngress)
                .execute();
        System.out.println("Created ingress " + result.getMetadata().getName());
        // TODO: monitor the status of this ingress to see when the pods it created are ready.
        return result;
    }

    private void copyBasics(final String ingressName, final V1Ingress ingress, final V1Ingress newIngress) {
        final V1ObjectMeta meta = new V1ObjectMeta();
        meta.setName(ingressName);
        final Map<String, String> annos = new LinkedHashMap<>();
        for (Map.Entry<String, String> annotation : ingress.getMetadata().getAnnotations().entrySet()) {
            annos.put(annotation.getKey(), annotation.getValue());
        }
        meta.setAnnotations(annos);

        newIngress.setMetadata(meta);
    }

    public V1Service createService(final String userName) throws ApiException {
        final V1ObjectMeta metadata = new V1ObjectMeta();
        final Map<String, String> annos = new LinkedHashMap<>();
        annos.put(ANNO_BACKEND_CONFIG, BACKEND_ENVOY);
        annos.put(ANNO_APP_PROTOCOLS, PROTOCOL_HTTP2);
        annos.put(ANNO_NEG, NEG_INGRESS);
        metadata.setAnnotations(annos);
        final LinkedHashMap<String, String> labels = new LinkedHashMap<>();
        labels.put(LABEL_USER, userName);
        labels.put(LABEL_PURPOSE, PURPOSE_WORKER);
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

        final V1Service createdSvc = api.createNamespacedService(NAMESPACE, newSvc, null, null, null);
        System.out.println("Created service " + createdSvc.getMetadata().getName());
        return createdSvc;
    }

    private DhWorker setupPod(final V1Pod pod, final String ingressName) throws KubectlException {
        String userName = NameGen.newName();
        String podName = pod.getMetadata().getName();
        V1Pod result;
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
            return null;
        }

        // hokay! we got back a result, assume we may _not_ have won a race!
        final V1Pod check = Kubectl.get(V1Pod.class)
                .apiClient(apiClient)
                .namespace(NAMESPACE)
                .name(podName)
                .execute();

        final String realLabel = check.getMetadata().getLabels().get(NameConstants.LABEL_USER);
        if (userName.equals(realLabel)) {
            System.out.println(NameGen.getMyName() + " won pod " + podName +" for user " + userName);
            final DhWorker worker = new DhWorker(userName, pod.getMetadata().getName(), userName, ingressName, userName + "." + NameConstants.DOMAIN);
            return worker;
        }
        System.out.println(NameGen.getMyName() + " lost pod " + podName +" for user " + userName);
        // This weird log is here so we can detect if race conditions actually happen, or if we'll know we lost immediately
        System.out.println("Pre-check label: " + result.getMetadata().getLabels().get(NameConstants.LABEL_USER));
        return null;
    }

    private V1Deployment getDeployment() {
        KubectlGet<V1Deployment>.KubectlGetSingle getDeploy = Kubectl.get(V1Deployment.class)
                .namespace(NameConstants.DH_NAMESPACE)
                .name(NameConstants.NAME_DEPLOYMENT)
                ;
        getDeploy.apiClient(apiClient);
        try {
            return getDeploy.execute();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("No deployment named " + NameConstants.NAME_DEPLOYMENT + "; have you run helm install yet?");
        }
    }

    public OkHttpClient getHttpClient() {
        return httpClient;
    }

    public boolean claimPod(final KubeSession session, final V1Pod pod) throws KubectlException, ApiException {
        if (session.getPod() != null) {
            return false;
        }
        final String podName = pod.getMetadata().getName();
        session.setPod(pod);

        // now, also set the dh.user label on the given pod
        final KubectlPatch<V1Pod> patchCmd = Kubectl.patch(V1Pod.class)
                .apiClient(apiClient)
                .namespace(DH_NAMESPACE)
                .name(podName);

        String patchBody = "[{\"op\":\"add\",\"path\":\"/metadata/labels/" + LABEL_USER + "\", \"value\": \"" + session.getUserName() + "\" }]";
        final V1Patch patch = new V1Patch(patchBody);
//        final V1Pod result = api.patchNamespacedPod(podName, DH_NAMESPACE, patch, null, null, null, null);
        patchCmd.patchContent(patch);
        patchCmd.patchType("json");
        final V1Pod result = patchCmd.execute();
        String resultLabel = result.getMetadata().getLabels().get(LABEL_USER);

        return session.getUserName().equals(resultLabel);
    }
}
