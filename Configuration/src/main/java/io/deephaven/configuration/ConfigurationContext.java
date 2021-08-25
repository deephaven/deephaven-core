package io.deephaven.configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * A class for determining the environment that a configuration is running within.
 */
class ConfigurationContext {
    // Note that we explicitly use 'host' as a value in the configuration files.
    // If a system property named 'host' exists, it will be ignored.
    private static final String HOSTNAME = "host";
    // Note that we explicitly allow the use of 'process' as a shorthand for 'process.name' in the
    // configuration files.
    // If a system property named 'process' exists, it will be ignored in favor of 'process.name'.
    static final String PROCESS_NAME_PROPERTY = "process";

    final private Map<String, String> contextItems;
    final private Collection<String> hostOptions;

    /**
     * Create a ConfigurationContext and preload the non-system tokens.
     */
    public ConfigurationContext() {
        contextItems = new HashMap<>();
        hostOptions = new ArrayList<>();
        contextItems.put(PROCESS_NAME_PROPERTY, getSystemProperty("process.name"));
    }

    /**
     * Check whether the current system context matches one of the requested values in the specified
     * scope
     * 
     * @param token The name of the property to check
     * @param targetValues A list of possible values for the specified property, such as
     *        'process.name=foo'
     * @return True if the specified property currently has a value equal to one of the target
     *         values, false otherwise.
     */
    public boolean matches(final String token, final List<String> targetValues) {
        // Mostly this is just checking system properties, but we also have one special case; the
        // hostname is allowed to
        // be several different potential values (short name, FQDN, etc), so we have to check that
        // separately.
        if (token.toLowerCase().equals(HOSTNAME)) {
            if (hostOptions.isEmpty()) {
                populateHostnames();
            }
            for (String aHostName : hostOptions) {
                if (targetValues.contains(aHostName.toLowerCase()))
                    return true;
            }
            return false;
        } else {
            // check that the token value in the context matches for the current scope item
            final String contextValue = getContextItem(token);
            if (contextValue == null)
                return false;
            return targetValues.contains(contextValue);
        }
    }

    /**
     * Retrieve a specified context item. These are usually but not necessarily system properties or
     * a small number of other environmental factors.
     * 
     * @param token The name of the context item to look up.
     * @return The current value of the specified context item, or null if no value exists.
     */
    private String getContextItem(final String token) {
        if (contextItems.containsKey(token)) {
            return contextItems.get(token);
        }
        // If we don't have an existing context item by this name, see if we have a matching system
        // property
        return getSystemProperty(token);
    }

    /**
     * Get the hostname of the current system where this is running, along with the IP address and
     * fully-qualified name.
     */
    private void populateHostnames() {
        try {
            final InetAddress address = InetAddress.getLocalHost();
            // allow the machine to be identified by its hostname
            final String hostName = address.getHostName();
            hostOptions.add(hostName);

            // Allow the machine to be identified by its IP address
            final String numericalAddress = address.getHostAddress();
            hostOptions.add(numericalAddress);

            // Allow the machine to be specified by the fully-qualified domain name - if available.
            // If we can't get the FQDN, this just returns the IP address, so don't repeat that.
            final String fqdn = address.getCanonicalHostName();
            if (!fqdn.equals(numericalAddress)) {
                hostOptions.add(address.getCanonicalHostName());
            }
            // Also allow the machine to be specified by the simple machine name, if one exists.
            final int dotPosition = hostName.indexOf(".");
            // If the name is X.y, then also allow just the leading 'X'
            if (dotPosition > 0) {
                // noinspection RedundantStringOperation
                hostOptions.add(address.getHostName().substring(0, dotPosition));
            }

        } catch (UnknownHostException e) {
            // If somehow we can't get the current host name, then don't apply any host-specific
            // parameters.
            System.err.println(
                "Unable to get current host name. Host-specific configuration items will be ignored.");
        }
    }

    /**
     * Retrieve and store a specified system property's value.
     * 
     * @param propertyName The system property to look up. If a value exists, it will be cached for
     *        later retrieval.
     * @return The value of the requested system property, or null if the property has no set value.
     */
    private String getSystemProperty(final String propertyName) {
        final String propVal = System.getProperty(propertyName);
        if (propVal != null) {
            contextItems.put(propertyName, propVal);
        }
        return propVal;
    }

    /**
     * Return the configuration contexts. This is the list of system properties that may have been
     * used to parse the configuration file. This collection will be immutable.
     *
     * @return the configuration contexts.
     */
    Collection<String> getContextKeyValues() {
        // Create a new HashSet, so that changes to the underlying contextItems don't find their way
        // back to the caller
        return Collections.unmodifiableCollection(new HashSet<>(contextItems.keySet()));
    }
}
