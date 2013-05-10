package com.nearinfinity.honeycomb.config;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class HoneycombConfiguration {
    private final Map<String, Map<String, String>> adapters;
    private AdapterType defaultAdapter;

    public HoneycombConfiguration(Map<String, Map<String, String>> adapters,
                                  String defaultAdapter) {
        this.adapters = adapters;
        this.defaultAdapter = AdapterType.valueOf(defaultAdapter.toUpperCase());
    }

    public boolean isAdapterConfigured(AdapterType adapter) {
        checkNotNull(adapter);
        return adapters.containsKey(adapter.getName());
    }

    public Map<String, String> getAdapterOptions(AdapterType adapter) {
        checkNotNull(adapter);
        return adapters.get(adapter.getName());
    }

    public AdapterType getDefaultAdapter() {
        return defaultAdapter;
    }
}
