package com.criteo.nosql.casspoke.cassandra;

import com.criteo.nosql.casspoke.discovery.Service;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import com.datastax.driver.core.Host;

import java.net.InetSocketAddress;
import java.util.Map;

public class CassandraMetrics implements AutoCloseable
{
    // TODO: public for tests. It has to be improved
    public static final Gauge UP = Gauge.build()
            .name("cassandra_up")
            .help("Are the servers up?")
            .labelNames("cluster", "instance", "datacenter")
            .create().register();

    // TODO: public for tests. It has to be improved
    public static final Summary LATENCY = Summary.build()
            .name("cassandra_latency")
            .help("latencies observed by instance and command")
            .labelNames("cluster", "instance", "command", "datacenter")
            .maxAgeSeconds(5 * 60)
            .ageBuckets(5)
            .quantile(0.5, 0.05)
            .quantile(0.9, 0.01)
            .quantile(0.99, 0.001)
            .register();

    private final String clusterName;

    public CassandraMetrics(final Service service) {
        this.clusterName = service.getClusterName();
    }

    public void updateAvailability(Map<Host, Boolean> availabilities) {
        availabilities.forEach((host, availability) -> {
            UP      .labels(clusterName, host.getSocketAddress().getHostString(), host.getDatacenter())
                    .set(availability ? 1 : 0);
        });
    }

    public void updateGetLatency(final Map<Host, Long> latencies)
    {
        latencies.forEach((host, latency) -> {
            LATENCY .labels(clusterName, host.getSocketAddress().getHostString(), "get", host.getDatacenter())
                    .observe(latency);
        });
    }

    public void updateSetLatency(final Map<Host, Long> latencies)
    {
        latencies.forEach((host, latency) -> {
            LATENCY .labels(clusterName, host.getSocketAddress().getHostString(), "set", host.getDatacenter())
                    .observe(latency);
        });
    }

    public void close() {
        // FIXME we should remove only metrics with the label cluster=clustername
        clear();
    }

    public static void clear() {
        UP.clear();
        LATENCY.clear();
    }
}