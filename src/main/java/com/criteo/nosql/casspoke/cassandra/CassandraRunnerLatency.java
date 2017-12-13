package com.criteo.nosql.casspoke.cassandra;

import com.criteo.nosql.casspoke.config.Config;
import com.criteo.nosql.casspoke.discovery.IDiscovery;

import java.util.Collections;

public class CassandraRunnerLatency extends CassandraRunnerAbstract {

    public CassandraRunnerLatency(Config cfg, IDiscovery discovery, long refreshDiscoveryInMs) {
        super(cfg, discovery, refreshDiscoveryInMs);
    }

    public void poke() {
        monitors.forEach((service, monitor) -> {
            final CassandraMetrics m = metrics.get(service);
            m.updateGetLatency(monitor.map(CassandraMonitor::collectGetLatencies).orElse(Collections.emptyMap()));
            m.updateSetLatency(monitor.map(CassandraMonitor::collectSetLatencies).orElse(Collections.emptyMap()));
        });
    }
}
