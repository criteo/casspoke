package com.criteo.nosql.casspoke.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Configuration class that are use by the app
 * Use fromFile method in order to transform a yaml file into an config's instance
 * <p>
 * TODO (r.gerard): Improve the class, add substructure for consul and services instead of Map<String,String>
 */
public final class Config {

    public static final String DEFAULT_PATH = "config.yml";

    private Map<String, String> app;
    private Discovery discovery;

    public static Config fromFile(String filePath) throws IOException {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(filePath), Config.class);
    }

    public Discovery getDiscovery() {
        return discovery;
    }

    public Map<String, String> getApp() {
        return app;
    }

    public static class ConsulDiscovery {
        private String host = "localhost";
        private int port = 8500;
        private int timeoutInSec = 10;
        private String readConsistency = "STALE";
        private List<String> tags = Collections.EMPTY_LIST;

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public int getTimeoutInSec() {
            return timeoutInSec;
        }

        public String getReadConsistency() {
            return readConsistency;
        }

        public List<String> getTags() {
            return tags;
        }
    }

    public static class DnsEntry {
        private String clustername;
        private String host;
        public String getClustername() {
            return clustername;
        }

        public String getHost() {
            return host;
        }
    }

    public static class Discovery {
        private ConsulDiscovery consul;
        private List<DnsEntry> dns;

        public ConsulDiscovery getConsul() {
            return consul;
        }

        public List<DnsEntry> getDns() { return dns; }
    }
}

