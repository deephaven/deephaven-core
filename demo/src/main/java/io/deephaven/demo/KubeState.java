package io.deephaven.demo;

import io.kubernetes.client.extended.kubectl.exception.KubectlException;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1Ingress;
import io.kubernetes.client.openapi.models.V1IngressRule;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.demo.NameConstants.DOMAIN;
import static io.deephaven.demo.NameConstants.LABEL_USER;

/**
 * KubeState: an immutable snapshot of the "State of the world of Kubernetes objects we care about".
 * <p>
 * <p> Only one instance of {@link KubeManager} should create and maintain KubeState instances.
 * <p> This pod is labeled as {@link NameConstants#LABEL_PURPOSE}={@link NameConstants#PURPOSE_CONTROLLER}
 * <p> (dh.purpose = controller)
 * <p> and is defined in demo/helm/templates/control-deployment.yaml.
 */
public class KubeState {

    private final ConcurrentMap<String, KubeSession> sessions = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Consumer<KubeState>> onRefresh = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<V1Pod> unclaimedPods = new ConcurrentLinkedQueue<>();

    public List<Throwable> computeNow(KubeManager manager) {
        // read in all worker pods, all services, and all ingresses... at the same time.
        List<Throwable> failures = new ArrayList<>();
        Runnable[] readTasks = {
                ()-> {
                    try {
                        final List<V1Pod> pods = manager.getWorkerPods();
                        copyPodsToSession(pods);
                    } catch (Throwable t) {
                        failures.add(t);
                    }
                },
                ()-> {
                    try {
                        final List<V1Service> services = manager.getWorkerServices();
                        copyServicesToSession(services);
                    } catch (Throwable t) {
                        failures.add(t);
                    }

                },
                ()-> {
                    try {
                        final List<V1Ingress> ingresses = manager.getWorkerIngress();
                        copyIngressesToSession(ingresses);
                    } catch (Throwable t) {
                        failures.add(t);
                    }
                },
        };
        Stream.of(readTasks).parallel().forEach(Runnable::run);
        // once we're done reading everything, do an analysis pass.
        try {
            analyzeEverything(manager);
        } catch (Exception e) {
            failures.add(e);
        }
        // we don't parallelize callbacks, so you can have reliable state ordering.
        for (Consumer<KubeState> refresh : onRefresh) {
            refresh.accept(this);
        }
        return failures;
    }

    private void analyzeEverything(final KubeManager manager) throws KubectlException {
        ConcurrentLinkedQueue<KubeSession> needIngress = new ConcurrentLinkedQueue<>();
        sessions.values().parallelStream().forEach(session -> {

            if (!session.isComplete()) {
                // when sessions are incomplete, attempt to fill in the gaps
                while (session.getPod() == null) {
                    V1Pod readyToGo = unclaimedPods.poll();
                    if (readyToGo == null) {
                        // nothing is ready to go.  consider scale up?
                        return;
                    } else {
                        // claim this pod for the given session
                        try {
                            if (manager.claimPod(session, readyToGo)) {
                                break;
                            }
                        } catch (KubectlException | ApiException e) {
                            System.err.println("Failure claiming pod " + readyToGo.getMetadata().getName() + " for " + session);
                            e.printStackTrace();
                        }
                    }
                }
                if (session.getService() == null) {
                    try {
                        final V1Service result = manager.createService(session.getUserName());
                        session.setService(result);
                    } catch (ApiException e) {
                        System.err.println("Failed to create missing service for " + session);
                    }
                    System.err.println("No service for session: " + session);
                }
                if (session.getIngress() == null) {
                    // build up a new ingress object...
                    needIngress.add(session);
                }
            }

        });
        if (!needIngress.isEmpty()) {
            System.out.println("IN NEED OF INGRESS: " + needIngress);
//            String newIngress = manager.newIngressName();
//            final V1Ingress result = manager.createIngress(newIngress,
//                    needIngress.stream().map(KubeSession::getUserName).collect(Collectors.toList()));
//            for (KubeSession ingress : needIngress) {
//                ingress.setIngress(result);
//            }
        }

    }

    private void copyIngressesToSession(final List<V1Ingress> ingresses) {

        ingresses.stream().parallel().forEach(ingress -> {

            assert ingress.getMetadata() != null : "Ingress sent without metadata? " + ingress;
            String ingressName = ingress.getMetadata().getName();

            assert ingress.getSpec() != null : "Ingress " + ingressName + " sent without spec? " + ingress;
            final List<V1IngressRule> rules = ingress.getSpec().getRules();
            // it is legal to have no rules; one may have a single ingress defining only a default provider (the controller)
            // thus is the rules are null, we return from this callback
            if (rules == null) {
                return;
            }
            // loop through rules,
            rules.parallelStream().forEach(rule -> {
                String fullHost = rule.getHost();
                if (fullHost == null) {
                    // ignore anyone rules w/o a host
                    return;
                }
                if (DOMAIN.equals(fullHost)) {
                    // ignore the root url, that goes to the controller
                    return;
                }
                int unameIndex = fullHost.indexOf('.');
                String userName = fullHost.substring(0, unameIndex);

                KubeSession session = sessions.computeIfAbsent(userName, KubeSession::new);
                session.setIngress(ingress);

            });
        });

    }

    private void copyServicesToSession(final List<V1Service> services) {
        services.stream().parallel().forEach(service -> {

            assert service.getMetadata() != null : "Service sent without metadata? " + service;
            String serviceName = service.getMetadata().getName();

            final Map<String, String> labels = service.getMetadata().getLabels();
            assert labels != null : "Service " + serviceName + " sent without labels? " + service;
            String userName = labels.get(LABEL_USER);

            if (userName == null) {
                System.out.println("Service " + serviceName + " did not have a label " + LABEL_USER + " in labels: " + labels);
            } else {
                KubeSession session = sessions.computeIfAbsent(userName, KubeSession::new);
                session.setService(service);
            }
        });
    }

    private void copyPodsToSession(final List<V1Pod> pods) {
        pods.stream().parallel().forEach(pod -> {

            assert pod.getMetadata() != null : "Pod sent without metadata? " + pod;
            String podName = pod.getMetadata().getName();

            final Map<String, String> labels = pod.getMetadata().getLabels();
            assert labels != null : "Pod " + podName + " sent without labels? " + pod;
            String userName = labels.get(LABEL_USER);


            if (userName == null) {
                // hm... consider claiming this item for any sessions w/o a pod?
                System.out.println("Pod " + podName + " did not have a label " + LABEL_USER + " in labels: " + labels);
                unclaimedPods.add(pod);
            } else {
                KubeSession session = sessions.computeIfAbsent(userName, KubeSession::new);
                session.setPod(pod);
            }
        });
    }

    public Stream<KubeSession> getSessions() {
        return sessions.values().stream();
    }

    public boolean hasSession(final String userName) {
        return sessions.containsKey(userName);
    }
}
