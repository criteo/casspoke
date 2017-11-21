package com.criteo.nosql.casspoke.cassandra;

import com.criteo.nosql.casspoke.consul.Service;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;

public class CassandraMonitor implements AutoCloseable {
    private static Logger logger = LoggerFactory.getLogger(CassandraMonitor.class);

    private final Service service;
    private final Cluster cluster;
    private final long timeoutInMs;
    private final Session session;

    private CassandraMonitor(final Service service, final Cluster cluster, long timeoutInMs) {
        this.service = service;
        this.cluster = cluster;
        this.timeoutInMs = timeoutInMs;
        this.session = cluster.connect();
    }

    public Map<InetSocketAddress, Boolean> collectAvailability() {

        final Map<InetSocketAddress, Boolean> availabilities = new HashMap<>();

        Session.State state = session.getState();
        for (Host host : cluster.getMetadata().getAllHosts()) {
            int connections = state.getOpenConnections(host);
            availabilities.put(host.getSocketAddress(), connections > 0);
            logger.debug("%s connections=%d\n", host, connections);
        }

        return availabilities;
    }

    public static Optional<CassandraMonitor> fromNodes(final Service service, Set<InetSocketAddress> endPoints, long timeoutInMs) {
        if(endPoints.isEmpty()) {
            return Optional.empty();
        }

        try {
            PoolingOptions poolingOptions = new PoolingOptions();

            poolingOptions
                    .setConnectionsPerHost(HostDistance.LOCAL,  1, 2)
                    .setConnectionsPerHost(HostDistance.REMOTE, 1, 2);

            final Cluster cluster = Cluster.builder()
                    .addContactPointsWithPorts(endPoints)
                    .withPoolingOptions(poolingOptions)
                    .withLoadBalancingPolicy(new RoundRobinPolicy())
                    .build();
            return Optional.of(new CassandraMonitor(service, cluster, timeoutInMs));
        } catch (Exception e) {
            logger.error("Cannot create connection to cluster", e);
            return Optional.empty();
        }

    }

    @Override
    public void close() {
        if (cluster != null) cluster.close();
    }

}
