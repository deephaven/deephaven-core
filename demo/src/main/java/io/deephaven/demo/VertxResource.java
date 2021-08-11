package io.deephaven.demo;

import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;

import javax.inject.Inject;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.cert.X509Certificate;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * VertxResource:
 * <p>
 * <p>
 * Created by James X. Nelson (James@WeTheInter.net) on 04/08/2021 @ 1:39 a.m..
 */
@Path("/")
public class VertxResource {

    private final Vertx vertx;
    private final WebClient client;

    @Inject
    public VertxResource(Vertx vertx) {
        this.vertx = vertx;
        client = io.vertx.mutiny.ext.web.client.WebClient.create(vertx);
        System.out.println("Starting demo-server; dumping environment:");
        // forcibly require the CertFactory main to be included...
        if (CertFactory.class.isAssignableFrom(Object.class)) {
            try {
                CertFactory.main("");
            } catch (Throwable ignore) { }
        }
        for (Map.Entry<String, String> e : System.getenv().entrySet()) {
            System.out.println(e.getKey() + " = " + e.getValue());
        }
    }

    @GET
    @Path("/health")
    public Uni<String> performHealthCheck(HttpServerRequest req) {
        // TODO: use the client to query running grpc-api server!
//        client.getAbs("grpc-api:8888").send()
//                .onItem().transform(HttpResponse::bodyAsString);

        // This env var is set by kubernetes when we run as a sidecar
        String grpcIp = System.getenv("DH_GRPC_PORT_8888_TCP_ADDR");
        // With this IP, we should ensure there are generated certs...
        System.out.println("My uri: " + req.uri());
        System.out.println("My headers: " + req.headers());

        try {
            final X509Certificate[] chain = req.peerCertificateChain();
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
        } catch (SSLPeerUnverifiedException | NullPointerException e) {
            e.printStackTrace();
        }

        vertx.setTimer(100, t-> {
            if (!req.isEnded()) {
                req.end();
            }
        });
        return Uni.createFrom().item(()-> {
            return "READY";
        });
    }



}
