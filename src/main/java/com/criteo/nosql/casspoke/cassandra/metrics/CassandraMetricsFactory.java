package com.criteo.nosql.casspoke.cassandra.metrics;

import com.criteo.nosql.casspoke.cassandra.metrics.builders.LatencyMetricBuilder;
import com.criteo.nosql.casspoke.cassandra.metrics.builders.UpMetricBuilder;
import com.criteo.nosql.casspoke.config.Config;
import com.criteo.nosql.casspoke.discovery.Service;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;

import java.util.Optional;

public class CassandraMetricsFactory {

    private static volatile CassandraMetricsFactory instance;

    private final Gauge up;
    private final Summary latency;
    private final Optional<String> probeLocation;

    private CassandraMetricsFactory(Config cfg) {
        this.up = new UpMetricBuilder(cfg).build();
        this.latency = new LatencyMetricBuilder(cfg).build();
        this.probeLocation = Optional.ofNullable(cfg.getApp().get("probeLocation"));
    }

    public static CassandraMetricsFactory getInstance(Config cfg) {
        CassandraMetricsFactory result = instance;
        if (result != null) {
            return result;
        }
        synchronized(CassandraMetricsFactory.class) {
            if (instance == null) {
                instance = new CassandraMetricsFactory(cfg);
            }
            return instance;
        }
    }

    public CassandraMetrics createMetrics(Service service) {
        return probeLocation
                .map(location -> new CassandraMetrics(service.getClusterName(), location, up, latency))
                .orElseGet(() -> new CassandraMetrics(service.getClusterName(), up, latency));
    }

}
