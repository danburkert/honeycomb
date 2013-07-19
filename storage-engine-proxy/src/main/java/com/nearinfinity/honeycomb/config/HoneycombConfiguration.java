package com.nearinfinity.honeycomb.config;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Wrapper class for Honeycomb configuration properties.  Provides helper
 * methods for accessing configuration properties concerning the Honeycomb proxy.
 * Individual backends are passed the properties map upon creation and are not
 * expected to use this class.
 */
public class HoneycombConfiguration {

    private final Map<String, String> properties;

    public HoneycombConfiguration(Map<String, String> properties) {
        this.properties = ImmutableMap.copyOf(properties);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public BackendType getDefaultBackend() {
        String backend = properties.get(Constants.DEFAULT_BACKEND_PROP);
        checkState(backend != null, "The " + Constants.DEFAULT_BACKEND_PROP + " property is not configured");
        return BackendType.valueOf(backend.trim().toUpperCase());
    }

    public boolean isBackendEnabled(BackendType backend) {
        checkNotNull(backend);
        String prop = Constants.HONEYCOMB_NAMESPACE + "." + backend.getName() + ".enabled";
        String val = properties.get(prop);
        checkState(val != null, "The " + prop + " property is not configured");
        return val.trim().toLowerCase().equals("true");
    }

}
