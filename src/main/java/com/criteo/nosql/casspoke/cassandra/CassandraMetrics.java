package com.criteo.nosql.casspoke.cassandra;

import com.criteo.nosql.casspoke.consul.Service;
import io.prometheus.client.Gauge;

import java.net.InetSocketAddress;
import java.util.Map;

public class CassandraMetrics {

    static final Gauge UP = Gauge.build()
            .help("Are the servers up?")
            .name("cassandra_up")
            .labelNames("cluster", "instance")
            .create().register();

    private final String clusterName;

    public CassandraMetrics(final Service service) {
        this.clusterName = service.getClusterName();

    }

    public void updateAvailability(Map<InetSocketAddress, Boolean> stats) {
        stats.forEach((k, v) -> {
            UP.labels(clusterName, k.getHostName()).set(v ? 1 : 0);
        });
    }


    public void close() {
        UP.clear();
    }
}