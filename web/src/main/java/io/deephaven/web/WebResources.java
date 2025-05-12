//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web;

import com.google.auto.service.AutoService;
import io.deephaven.server.resources.ServerResource;

/**
 * Default resources for the Deephaven web server. Includes the JS API, the web UI, and JS example pages.
 */
@AutoService(ServerResource.class)
public class WebResources extends ServerResource {
    @Override
    public String getName() {
        return "deephaven-core-web";
    }
}
