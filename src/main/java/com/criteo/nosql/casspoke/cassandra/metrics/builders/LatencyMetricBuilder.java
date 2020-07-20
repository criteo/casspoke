package com.criteo.nosql.casspoke.cassandra.metrics.builders;

import com.criteo.nosql.casspoke.config.Config;
import io.prometheus.client.Summary;

import java.util.Optional;

public class LatencyMetricBuilder {

    private final Optional<String> labelPrefix;
    private final Optional<String> proberLocation;

    public LatencyMetricBuilder(Config cfg) {
        this.labelPrefix = Optional.ofNullable(cfg.getApp().get("labelPrefix"));
        this.proberLocation = Optional.ofNullable(cfg.getApp().get("proberLocation"));
    }

    public Summary build() {
        String cluster = withPrefixFromConfig("cluster");
        String instance = withPrefixFromConfig("instance");
        String command = withPrefixFromConfig("command");
        String datacenter = withPrefixFromConfig("datacenter");
        return proberLocation
                .map(location -> createSummary(cluster, instance, command, datacenter, location))
                .orElseGet(() -> createSummary(cluster, instance, command, datacenter));
    }

    private Summary createSummary(String... labelNames) {
        return Summary.build()
                .name("cassandra_latency")
                .help("latencies observed by instance and command")
                .labelNames(labelNames)
                .maxAgeSeconds(5 * 60)
                .ageBuckets(5)
                .quantile(0.5, 0.05)
                .quantile(0.9, 0.01)
                .quantile(0.99, 0.001)
                .register();
    }

    private String withPrefixFromConfig(String labelName) {
        return labelPrefix
                .map(prefix -> prefix + labelName)
                .orElse(labelName);
    }

}
