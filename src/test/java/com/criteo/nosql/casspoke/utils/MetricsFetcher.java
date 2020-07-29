package com.criteo.nosql.casspoke.utils;

import com.criteo.nosql.casspoke.cassandra.metrics.CassandraMetricsFactory;
import com.criteo.nosql.casspoke.config.Config;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;

import java.lang.reflect.Field;

public class MetricsFetcher {

    private final Gauge up;
    private final Summary latency;

    public MetricsFetcher(Config cfg) throws NoSuchFieldException, IllegalAccessException {
        CassandraMetricsFactory factory = CassandraMetricsFactory.getInstance(cfg);
        Field upField = factory.getClass().getDeclaredField("up");
        Field latencyField = factory.getClass().getDeclaredField("latency");
        upField.setAccessible(true);
        latencyField.setAccessible(true);
        this.up = (Gauge) upField.get(factory);
        this.latency = (Summary) latencyField.get(factory);
    }

    public Gauge getUp() {
        return up;
    }

    public Summary getLatency() {
        return latency;
    }
}
