package io.deephaven.configuration;

import java.util.*;
import java.util.stream.Collectors;

class ConfigurationScope {
    // region class variables
    final private String token;
    final private List<String> targetValues;
    final private List<ConfigurationScope> subScopes;
    // endregion class variables

    // region properties
    private String getToken() {
        return token;
    }

    private List<String> getTargetValues() {
        return targetValues;
    }
    // endregion properties

    // region constructors

    /**
     * Create a single scope item with a single token and a pipe-delimited list of target values.
     *
     * @param token The property name to check
     * @param targetValue A pipe-delimited list of values to match against the property
     */
    ConfigurationScope(String token, String targetValue) {
        this.token = token;
        subScopes = new ArrayList<>();
        targetValues = Arrays.stream(targetValue.split("\\|")).collect(Collectors.toList());
    }

    /**
     * Create a scope with a set of subscopes. Collapse the subscopes to this scope if there is only one subscope.
     *
     * @param scopes The list of scopes to evaluate collectively.
     */
    ConfigurationScope(List<ConfigurationScope> scopes) {
        if (scopes.size() == 1) {
            this.token = scopes.get(0).getToken();
            this.targetValues = scopes.get(0).getTargetValues();
            subScopes = new ArrayList<>();
        } else {
            this.token = null;
            this.targetValues = null;
            subScopes = scopes;
        }

    }
    // endregion constructors

    /**
     * Determine whether the specified context is within this scope.
     *
     * @param context The ConfigurationContext to check
     * @return True if the parameters of this scope match the context, false otherwise.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    boolean scopeMatches(ConfigurationContext context) {
        if (subScopes.isEmpty()) {
            return context.matches(token, targetValues);
        } else {
            // check that this passes all sub-scopes
            for (ConfigurationScope aScope : subScopes) {
                if (!aScope.scopeMatches(context)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Represent a ConfigurationScope as either the token and values it indicates, or a collection of subscopes. Useful
     * when debugging.
     *
     * @return The String representation of this scope.
     */
    @Override
    public String toString() {
        if (subScopes.isEmpty()) {
            return token + "=" + String.join("|", targetValues);
        } else {
            return "[" + subScopes.stream().map(Objects::toString).collect(Collectors.joining(",")) + "]";
        }
    }
}
