//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.resources;

import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Helper to load resources based on configuration. Any resource specified in configuration may be give a boolean
 * "enabled" with true to allow the resources to be served and false to have them be excluded, with a default of true.
 * Resources may also specify a positive integer "priority" to determine the order in which they are served, lowest
 * first, allowing more important resources to override others. Default priority is {@value Integer#MAX_VALUE}.
 */
public class ServerResources {
    private static final Logger log = LoggerFactory.getLogger(ServerResources.class);
    private static final String CONFIG_PREFIX = "http.resources.";

    public static List<String> resourcesFromServiceLoader(Configuration configuration) {
        Properties properties = configuration.getProperties(CONFIG_PREFIX);

        List<ServerResource> resources = ServiceLoader.load(ServerResource.class).stream()
                .map(ServiceLoader.Provider::get)
                .collect(Collectors.toList());

        Map<String, Integer> priorities = new HashMap<>();
        for (ServerResource resource : resources) {
            Object enabled = properties.get(resource.getName() + ".enabled");
            if (enabled == null || Boolean.parseBoolean(enabled.toString())) {
                Object priority = properties.get(resource.getName() + ".priority");
                if (priority == null) {
                    priorities.put(resource.getName(), Integer.MAX_VALUE);
                } else {
                    priorities.put(resource.getName(), Integer.parseInt(priority.toString()));
                }
            }
        }

        return resources.stream()
                .filter(res -> priorities.containsKey(res.getName()))
                .sorted(Comparator.comparing(res -> priorities.get(res.getName())))
                .peek(resource -> {
                    log.debug().append("Loading resource: ").append(resource.getName()).append(" with priority: ")
                            .append(priorities.get(resource.getName())).endl();
                })
                .map(ServerResource::getResourceBaseUrl)
                .collect(Collectors.toList());
    }
}
