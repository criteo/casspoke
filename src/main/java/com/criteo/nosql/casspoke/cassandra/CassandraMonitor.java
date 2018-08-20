package com.criteo.nosql.casspoke.cassandra;

import com.criteo.nosql.casspoke.discovery.Service;
import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CassandraMonitor implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CassandraMonitor.class);

    private final Service service;
    private final Cluster cluster;
    private final WhiteLBPolicy lbPolicy;
    private final int timeoutInMs;
    private final Session session;
    private final UUID sessionId = UUIDs.random();
    private final BoundStatement setRequest;
    private final BoundStatement getRequest;

    private CassandraMonitor(final Service service, final Cluster cluster, final WhiteLBPolicy lbPolicy, final int timeoutInMs) {
        this.service = service;
        this.cluster = cluster;
        this.lbPolicy = lbPolicy;
        this.timeoutInMs = timeoutInMs;
        this.session = cluster.connect();
        this.setRequest = this.session.prepare("INSERT INTO system_traces.events (session_id, event_id, activity, source)" +
                                                "VALUES (?, now(), 'casspoke set latency measure', '127.0.0.1' ) USING TTL 60 ;")
                .setConsistencyLevel(ConsistencyLevel.ONE)
                .bind(this.sessionId);

        this.getRequest = this.session.prepare("SELECT * FROM system.local LIMIT 1")
                .setConsistencyLevel(ConsistencyLevel.ONE)
                .bind();
    }

    public static Optional<CassandraMonitor> fromNodes(final Service service, Set<InetSocketAddress> endPoints, int timeoutInMs) {
        if (endPoints.isEmpty()) {
            return Optional.empty();
        }

        try {
            final PoolingOptions poolingOptions = new PoolingOptions()
                    .setConnectionsPerHost(HostDistance.LOCAL, 1, 2)
                    .setConnectionsPerHost(HostDistance.REMOTE, 1, 2)
                    .setHeartbeatIntervalSeconds(20)
                    .setPoolTimeoutMillis((int) TimeUnit.SECONDS.toMillis(30));

            final WhiteLBPolicy lbPolicy = new WhiteLBPolicy();

            final Cluster cluster = Cluster.builder()
                    .addContactPointsWithPorts(endPoints)
                    .withPoolingOptions(poolingOptions)
                    .withLoadBalancingPolicy(lbPolicy)
                    .withSocketOptions(new SocketOptions()
                            .setConnectTimeoutMillis((int) TimeUnit.SECONDS.toMillis(30))
                            .setTcpNoDelay(true)
                            .setKeepAlive(true)
                            .setReadTimeoutMillis(timeoutInMs)
                            .setReuseAddress(true)
                            .setSoLinger(timeoutInMs)
                    )
                    .build();
            return Optional.of(new CassandraMonitor(service, cluster, lbPolicy, timeoutInMs));
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

    public Map<InetSocketAddress, Long> collectGetLatencies() {
        final Map<InetSocketAddress, Long> getLatencies = new HashMap<>();
        for (int count = cluster.getMetadata().getAllHosts().size(); count > 0; count--) {

            long duration = timeoutInMs;
            try {
                final long start = System.nanoTime();
                session.execute(getRequest);
                duration = System.nanoTime() - start;
            } catch (Exception e) {
                logger.error("Error while reading from {} ", lbPolicy.theLastTargetedHost.get(), e);
            }
            final Host host = lbPolicy.theLastTargetedHost.get();
            getLatencies.put(host.getSocketAddress(), duration);
        }
        return getLatencies;
    }

    public Map<InetSocketAddress, Long> collectSetLatencies() {
        final Map<InetSocketAddress, Long> setLatencies = new HashMap<>();

        for (int count = cluster.getMetadata().getAllHosts().size(); count > 0; count--) {

            long duration = timeoutInMs;
            try {
                final long start = System.nanoTime();
                session.execute(this.setRequest);
                duration = System.nanoTime() - start;
            } catch (Exception e) {
                logger.error("Error while writing to {} ", lbPolicy.theLastTargetedHost.get(), e);
            }

            final Host host = lbPolicy.theLastTargetedHost.get();
            setLatencies.put(host.getSocketAddress(), duration);
        }
        return setLatencies;
    }

    @Override
    public void close() {
        if (cluster != null) cluster.close();
    }

}
