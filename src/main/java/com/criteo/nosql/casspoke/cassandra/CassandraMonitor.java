package com.criteo.nosql.casspoke.cassandra;

import com.criteo.nosql.casspoke.discovery.Service;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class CassandraMonitor implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CassandraMonitor.class);

    private final Service service;
    private final Cluster cluster;
    private final int timeoutInMs;
    private final Session session;

    private CassandraMonitor(final Service service, final Cluster cluster, int timeoutInMs) {
        this.service = service;
        this.cluster = cluster;
        this.timeoutInMs = timeoutInMs;
        this.session = cluster.connect();
    }

    public static Optional<CassandraMonitor> fromNodes(final Service service, Set<InetSocketAddress> endPoints, int timeoutInMs) {
        if (endPoints.isEmpty()) {
            return Optional.empty();
        }

        try {
            final PoolingOptions poolingOptions = new PoolingOptions()
                    .setConnectionsPerHost(HostDistance.LOCAL, 1, 2)
                    .setConnectionsPerHost(HostDistance.REMOTE, 1, 2);

            final Cluster cluster = Cluster.builder()
                    .addContactPointsWithPorts(endPoints)
                    .withPoolingOptions(poolingOptions)
                    .withLoadBalancingPolicy(new RoundRobinPolicy())
                    .withSocketOptions(new SocketOptions()
                            .setConnectTimeoutMillis(timeoutInMs))
                    .build();
            return Optional.of(new CassandraMonitor(service, cluster, timeoutInMs));
        } catch (Exception e) {
            logger.error("Cannot create connection to cluster", e);
            return Optional.empty();
        }

    }

    public Map<InetSocketAddress, Boolean> collectAvailability() {

        final Map<InetSocketAddress, Boolean> availabilities = new HashMap<>();

        final Session.State state = session.getState();
        for (Host host : cluster.getMetadata().getAllHosts()) {
            final int connections = state.getOpenConnections(host);
            availabilities.put(host.getSocketAddress(), connections > 0);
            logger.debug("%s connections=%d\n", host, connections);
        }

        return availabilities;
    }

    @Override
    public void close() {
        if (cluster != null) cluster.close();
    }

}
