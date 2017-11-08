package com.criteo.nosql.casspoke.cassandra;

import com.criteo.nosql.casspoke.consul.Consul;
import com.ecwid.consul.v1.health.model.HealthService;
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

    public CassandraMetrics(final HealthService.Service service) {
        this.clusterName = Consul.getClusterName(service);

    }

    public void updateAvailability(Map<InetSocketAddress, Boolean> stats) {
        stats.entrySet().stream().forEach(e -> {
            UP.labels(clusterName, (e.getKey()).getHostName()).set(e.getValue() ? 1 : 0);
        });
    }


    public void close() {
        UP.clear();
    }
}