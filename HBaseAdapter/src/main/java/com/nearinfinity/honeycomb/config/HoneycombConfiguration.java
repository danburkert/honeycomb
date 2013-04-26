package com.nearinfinity.honeycomb.config;

import com.nearinfinity.honeycomb.util.Verify;

import java.util.Map;

public class HoneycombConfiguration {
    private final Map<String, Map<String, String>> adapters;

    public HoneycombConfiguration(Map<String, Map<String, String>> adapters) {
        this.adapters = adapters;
    }

    public boolean isAdapterConfigured(String adapterName) {
        Verify.isNotNullOrEmpty(adapterName);
        return adapters.containsKey(adapterName);
    }

    public Map<String, String> getAdapterOptions(String adapterName) {
        Verify.isNotNullOrEmpty(adapterName);
        return adapters.get(adapterName);
    }
}
