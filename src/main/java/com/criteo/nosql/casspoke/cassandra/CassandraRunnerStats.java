package com.criteo.nosql.casspoke.cassandra;

import com.criteo.nosql.casspoke.config.Config;
import com.criteo.nosql.casspoke.discovery.IDiscovery;

import java.util.Collections;

public class CassandraRunnerStats extends CassandraRunnerAbstract {

    public CassandraRunnerStats(Config cfg, IDiscovery discovery, long refreshDiscoveryInMs) {
        super(cfg, discovery, refreshDiscoveryInMs);
    }

    public void poke() {
        monitors.forEach((service, monitor) -> {
            final CassandraMetrics m = metrics.get(service);
            m.updateAvailability(monitor.map(CassandraMonitor::collectAvailability).orElse(Collections.emptyMap()));
        });
    }
}
