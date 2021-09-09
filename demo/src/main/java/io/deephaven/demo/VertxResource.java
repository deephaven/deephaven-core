package io.deephaven.demo;

import io.kubernetes.client.extended.kubectl.exception.KubectlException;
import io.kubernetes.client.openapi.ApiException;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.Cookie;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;

import javax.inject.Inject;
import java.util.Map;

/**
 * VertxResource:
 * <p>
 * <p>
 * Created by James X. Nelson (James@WeTheInter.net) on 04/08/2021 @ 1:39 a.m..
 */
//@Path("/")
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

//    @GET
//    @Path("/health")
    public Uni<String> performHealthCheck(HttpServerRequest req) {
        // TODO: use the client to query running grpc-api server!
//        client.getAbs("grpc-api:8888").send()
//                .onItem().transform(HttpResponse::bodyAsString);

        // This env var is set by kubernetes when we run as a sidecar
        String grpcIp = System.getenv("DH_GRPC_PORT_8888_TCP_ADDR");
        // With this IP, we should ensure there are generated certs...
        System.out.println("My uri: " + req.uri());
        int delay = 100;
        vertx.setTimer(delay, t-> {
            if (!req.isEnded()) {
                System.out.println("Stream was not closed after "+ delay + "ms, closing it");
                req.end();
            }
        });
        return Uni.createFrom().item(()-> {
            return "READY";
        });
    }

    /**
     *
     * @param req - The http request to process
     * @return a Uni which either results in a redirect to a warm machine,
     * or renders a "please wait while your machine is prepared" message.
     */
//    @GET
//    @Path("s:.*")
    public void sendToSession(HttpServerRequest req) {

        Cookie cookie = req.getCookie("dh-user");
        String uname = null;
        if (cookie != null) {
            // TODO: verify that this cookie points to a running service, and if so, redirect to it.
            uname = cookie.getValue();
            if (hasValidRoute(uname)) {
                String uri = uname + "." + NameConstants.DOMAIN;
                req.response()
                        .putHeader("Location", uri)
                        .setStatusCode(302)
                        .end();
                return;
            } else {
                uname = null;
            }
        }
        try {
            uname = DhDemoServer.createIsolatedSession();
        } catch (ApiException | KubectlException e) {
            e.printStackTrace();
            req.response().end("Error creating session " + e);
            return;
        }
        String uri = uname + "." + NameConstants.DOMAIN;

        // okay... we should actually be blocking until this route is fully functional

        req.response().putHeader("Location", uri)
                      .setStatusCode(302)
                      .end();
        return;

        // A "fresh" visitor has arrived, check if they have headers routing them to an *online* service pod,
        // if valid, send them straight on to their ready-to-go-box with a redirect.

        // If there's no headers, or if the service has been deleted from kuberenetes cluster,
        // then we try to pull a warm, clean machine out of the pool for user to have.

        // If no such machine exists,
        // send user to a "please wait while you machine is prepared" page,
        // which will speak on the vert.x event bus to us, waiting for a machine to become available,
        // while we start a background thread to provision a new service.
        // Once the server thinks it's online, we should signal client webpage on vert.x event bus,
        // who will then poll until DNS for sure resolves (in case the pool of DNS names are all used),
        // and then, finally, send the user to their machine when we know the DNS is good and the page is loading.
    }

    private boolean hasValidRoute(final String uname) {
        // TODO: lookup an existing, healthy cluster route using this uname
        return false;
    }


}
