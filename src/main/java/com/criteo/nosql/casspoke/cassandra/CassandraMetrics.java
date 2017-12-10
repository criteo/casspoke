package com.criteo.nosql.casspoke.cassandra;

import com.criteo.nosql.casspoke.consul.Service;
import io.prometheus.client.Gauge;

import java.net.InetSocketAddress;
import java.util.Map;

public class CassandraMetrics implements AutoCloseable
{
    static final Gauge UP = Gauge.build()
            .name("cassandra_up")
            .help("Are the servers up?")
            .labelNames("cluster", "instance")
            .create().register();

    private final String clusterName;

    public CassandraMetrics(final Service service) {
        this.clusterName = service.getClusterName();
    }

    public void updateAvailability(Map<InetSocketAddress, Boolean> availabilities) {
        availabilities.forEach((addr, availability) -> {
            UP.labels(clusterName, addr.getHostName()).set(availability ? 1 : 0);
        });
    }

    public void close() {
        // FIXME we should remove only metrics with the label cluster=clustername
        UP.clear();
    }
}