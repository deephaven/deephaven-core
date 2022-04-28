package io.deephaven.demo.control;

import io.deephaven.demo.gcloud.GoogleDeploymentManager;
import io.deephaven.demo.api.Machine;
import io.deephaven.demo.manager.Execute;
import io.deephaven.demo.manager.NameConstants;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.vertx.core.http.Cookie;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.jboss.logging.Logger;

import javax.enterprise.inject.spi.CDI;
import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.demo.manager.NameConstants.*;

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
    private static String[] demoHtml;

    ClusterController controller;

    @Inject
    public DhDemoServer() {

    }

    public static void main(String... args)
        throws IOException, InterruptedException {

        final InputStream res = DhDemoServer.class.getResourceAsStream("index.html");

        demoHtml = new BufferedReader(
                new InputStreamReader(res, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n")).split("__URI__");
        Quarkus.run(DhDemoServer.class, args);
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

        controller = new ClusterController(new GoogleDeploymentManager("/tmp"), CONTROLLER);
        router.get("/health").handler(rc -> {
            LOG.info("Health check! " + rc.request().uri() + " : " +
                rc.request().headers().get("User-Agent") + " ( "
                + rc.request().headers().get("host") + " ) ");

            // TODO: real health check
            //    for workers, grpcurl is on cli, or we can use grpc client here in java
            //    for controllers, we should check if we have fresh metadata and no fatal errors.
            // TODO: handle built-in quarkus health check

            // enable cors to work on the main url, and the specific subdomain demo-candidate.demo.deephaven.app
            String allowedOrigin = "https://" + DOMAIN;
            if (("https://controller-" + VERSION_MANGLE + "." + DOMAIN) .equals(rc.request().getHeader("Origin"))) {
                allowedOrigin = "https://controller-" + VERSION_MANGLE + "." + DOMAIN;
            }
            LOG.infof("Request from origin %s allowed: %s", rc.request().getHeader("Origin"), allowedOrigin);
            rc.response()
                    .putHeader("Access-Control-Allow-Origin", allowedOrigin)
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
                    // disallow all robots... TODO: instead, send back a link-unfurling page
                    .end("User-agent: *\n" +
                            "Disallow: /");
        });
        router.get().handler(req -> {
            // for now, ssl redirects are handled by envoy
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
            if (userAgent == null || userAgent.contains("Bot") || userAgent.contains("Nimbostratus")) {
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
            // getting or creating a worker could take a while.
            // for now, we're going to let the browser window hang while we wait :'(
            // get off the vert.x event queue...
            LOG.info("Sending user off-thread to complete new machine request.");
            Cookie cookie = req.getCookie(NameConstants.COOKIE_NAME);
            Execute.setTimer("Claim Machine", () -> {

                if (cookie != null) {
                    String uname = cookie.getValue();
                    LOG.info("Handling request " + req.request().uri() + " w/ cookie " + uname);
                    // verify that this cookie points to a running service, and if so, redirect to it.

                    if (!uname.contains(".")) {
                        uname = uname + "." + DOMAIN;
                    }
                    if (controller.isMachineReady(uname)) {
                        String uri = "https://" + uname;
                        // if you re-visit the main url, we'll renew your 45 minute lease then send you back
                        if (controller.renewLease(uname)) {

                            String path = req.request().path();
                            if (!path.startsWith("/ide")) {
                                LOG.infof("Replacing path %s with /ide%s", path, path);
                                path = "/ide" + path;
                            }
                            String query = req.request().query();
                            if (query != null && query.length() > 0) {
                                path = path + "?" + query;
                            }
                            req.redirect(uri + path);
                            return;
                        }
                    }
                }

                LOG.info("Finding gcloud worker for user");
                handleGcloud(req);
            });

        });
        LOG.info("Serving controller on https://" + DOMAIN);
        Quarkus.waitForExit();
        return 0;
    }

    private void handleGcloud(final RoutingContext req) {
        final Machine machine = controller.requestMachine();
        String uri = "https://" + machine.getDomainName();
        LOG.infof("Sending user to %s", uri);
        // if we can reach /health immediately, the machine is ready, we should send user straight there
        final boolean isDev = "true".equals(System.getProperty("devMode"));
        if (isDev && controller.isMachineReady(machine.getDomainName())) {
            // devMode can skip cookies
            req.redirect(uri);
        } else {
            // always send user to interstitial page, so we can record our cookie before sending them along to their machine.
            String cookieDomain = System.getenv("COOKIE_DOMAIN");
            if (cookieDomain == null) {
                cookieDomain = DOMAIN + "; secure";
            }
            String path = req.request().path();
            String query = req.request().query();
            if (query != null && query.length() > 0) {
                path = path + "?" + query;
            }
            final String html = String.join(uri, demoHtml).replace("__PATH__", path);
            req.response()
                    .putHeader("content-type", "text/html")
                    .putHeader("x-frame-options", "DENY")
                    .putHeader("Set-Cookie", COOKIE_NAME + "=" + machine.getDomainName().split("[.]")[0] + "; Max-Age=2400; domain=" + cookieDomain + "; HttpOnly")
                    .setChunked(true)
                    .setStatusCode(200)
                    .end(html);
        }
    }

}
