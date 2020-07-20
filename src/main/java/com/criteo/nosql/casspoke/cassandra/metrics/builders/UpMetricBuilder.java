package com.criteo.nosql.casspoke.cassandra.metrics.builders;

import com.criteo.nosql.casspoke.config.Config;
import io.prometheus.client.Gauge;

import java.util.Optional;

public class UpMetricBuilder {

    private final Optional<String> labelPrefix;
    private final Optional<String> proberLocation;

    public UpMetricBuilder(Config cfg) {
        this.labelPrefix = Optional.ofNullable(cfg.getApp().get("labelPrefix"));
        this.proberLocation = Optional.ofNullable(cfg.getApp().get("proberLocation"));
    }

    public Gauge build() {
        String cluster = withPrefixFromConfig("cluster");
        String instance = withPrefixFromConfig("instance");
        String datacenter = withPrefixFromConfig("datacenter");
        return proberLocation
                .map(location -> createGauge(cluster, instance, datacenter, location))
                .orElseGet(() -> createGauge(cluster, instance, datacenter));
    }

    private Gauge createGauge(String... labelNames) {
        return Gauge.build()
                .name("cassandra_up")
                .help("Are the servers up?")
                .labelNames(labelNames)
                .create()
                .register();
    }

    private String withPrefixFromConfig(String labelName) {
        return labelPrefix
                .map(prefix -> prefix + labelName)
                .orElse(labelName);
    }

}
