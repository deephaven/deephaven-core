package io.deephaven.demo;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * HealthEndpoint:
 * <p>
 * <p>
 * Created by James X. Nelson (James@WeTheInter.net) on 04/08/2021 @ 1:28 a.m..
 */
@Path("/hello")
public class HealthEndpoint {

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        return "hello";
    }
}
