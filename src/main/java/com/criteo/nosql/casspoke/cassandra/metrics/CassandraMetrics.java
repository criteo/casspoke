package com.criteo.nosql.casspoke.cassandra.metrics;

import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import com.datastax.driver.core.Host;

import java.util.Map;
import java.util.Optional;

public class CassandraMetrics implements AutoCloseable {

    private final Gauge up;
    private final Summary latency;

    private final String clusterName;
    private final Optional<String> probeLocation;

    public CassandraMetrics(String clusterName, String probeLocation, Gauge up, Summary latency) {
        this.up = up;
        this.latency = latency;
        this.clusterName = clusterName;
        this.probeLocation = Optional.of(probeLocation);
    }

    public CassandraMetrics(String clusterName, Gauge up, Summary latency) {
        this.up = up;
        this.latency = latency;
        this.clusterName = clusterName;
        this.probeLocation = Optional.empty();
    }

    public void updateAvailability(Map<Host, Boolean> availabilities) {
        availabilities.forEach((host, availability) -> {
            String hostName = host.getSocketAddress().getHostName();
            String datacenter = host.getDatacenter();
            probeLocation
                    .map(location -> this.up.labels(clusterName, hostName, datacenter, location))
                    .orElseGet(() -> this.up.labels(clusterName, hostName, datacenter))
                    .set(availability ? 1 : 0);
        });
    }

    public void updateGetLatency(Map<Host, Long> latencies) {
        String command = "get";
        updateLatency(latencies, command);
    }

    public void updateSetLatency(Map<Host, Long> latencies) {
        String command = "set";
        updateLatency(latencies, command);
    }

    private void updateLatency(Map<Host, Long> latencies, String command) {
        latencies.forEach((host, latency) -> {
            String hostName = host.getSocketAddress().getHostName();
            String datacenter = host.getDatacenter();
            probeLocation
                    .map(location -> this.latency.labels(clusterName, hostName, command, datacenter, location))
                    .orElseGet(() -> this.latency.labels(clusterName, hostName, command, datacenter))
                    .observe(latency);
        });
    }

    public void close() {
        // FIXME we should remove only metrics with the label cluster=clustername
        clear();
    }

    public void clear() {
        this.up.clear();
        this.latency.clear();
    }
}