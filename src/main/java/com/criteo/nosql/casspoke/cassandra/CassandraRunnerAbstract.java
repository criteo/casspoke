package com.criteo.nosql.casspoke.cassandra;

import com.criteo.nosql.casspoke.config.Config;
import com.criteo.nosql.casspoke.discovery.Consul;
import com.criteo.nosql.casspoke.discovery.IDiscovery;
import com.criteo.nosql.casspoke.discovery.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;

public abstract class CassandraRunnerAbstract implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(CassandraRunnerAbstract.class);

    private final Config cfg;

    // consul
    private final IDiscovery discovery;
    private final long refreshDiscoveryInMs;
    // scheduler
    private final long tickRate;
    // app
    protected Map<Service, Set<InetSocketAddress>> services;
    protected Map<Service, Optional<CassandraMonitor>> monitors;
    protected Map<Service, CassandraMetrics> metrics;

    public CassandraRunnerAbstract(Config cfg, IDiscovery discovery, long refreshDiscoveryInMs) {
        this.cfg = cfg;

        this.discovery = discovery;
        this.refreshDiscoveryInMs = refreshDiscoveryInMs;

        this.tickRate = Long.parseLong(cfg.getApp().getOrDefault("tickRateInSec", "20")) * 1000L;

        this.services = Collections.emptyMap();
        this.monitors = Collections.emptyMap();
        this.metrics = Collections.emptyMap();
    }

    @Override
    public void run() {
        final List<EVENT> evts = Arrays.asList(EVENT.UPDATE_TOPOLOGY, EVENT.WAIT, EVENT.POKE);

        for (; ; ) {
            final long start = System.currentTimeMillis();
            final EVENT evt = evts.get(0);
            dispatch_events(evt);
            final long stop = System.currentTimeMillis();
            logger.info("{} took {} ms", evt, stop - start);

            rescheduleEvent(evt, start, stop);
            Collections.sort(evts, Comparator.comparingLong(event -> event.nexTick));
        }
    }

    private void rescheduleEvent(EVENT lastEvt, long start, long stop) {
        final long duration = stop - start;
        if (duration >= tickRate) {
            logger.warn("Operation took longer than 1 tick, please increase tick rate if you see this message too often");
        }

        EVENT.WAIT.nexTick = start + tickRate - 1;
        switch (lastEvt) {
            case WAIT:
                break;

            case UPDATE_TOPOLOGY:
                lastEvt.nexTick = start + refreshDiscoveryInMs;
                break;

            case POKE:
                lastEvt.nexTick = start + tickRate;
                break;
        }
    }

    private void dispatch_events(EVENT evt) {
        switch (evt) {
            case WAIT:
                try {
                    Thread.sleep(Math.max(evt.nexTick - System.currentTimeMillis(), 0));
                } catch (Exception e) {
                    logger.error("thread interrupted {}", e);
                }
                break;

            case UPDATE_TOPOLOGY:
                updateTopology();
                break;

            case POKE:
                poke();
                break;
        }
    }

    public void updateTopology() {
        final Map<Service, Set<InetSocketAddress>> new_services = discovery.getServicesNodesFor(cfg.getService().tags);

        // Consul down ?
        if (new_services.isEmpty()) {
            logger.info("Consul sent back no services to monitor. is it down ? Are you sure of your tags ?");
            return;
        }

        // Check if topology has changed
        if (Consul.areServicesEquals(services, new_services))
            return;

        logger.info("Topology changed, updating it");
        // Clean old monitors
        monitors.values().forEach(mo -> mo.ifPresent(CassandraMonitor::close));
        metrics.values().forEach(CassandraMetrics::close);

        // Create new ones
        services = new_services;
        monitors = services.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> CassandraMonitor.fromNodes(e.getKey(), e.getValue(), cfg.getService().timeoutInSec * 1000)));

        metrics = new HashMap<>(monitors.size());
        monitors.forEach((Service, v) -> metrics.put(Service, new CassandraMetrics(Service)));
    }

    abstract public void poke();

    private enum EVENT {
        UPDATE_TOPOLOGY(System.currentTimeMillis()),
        WAIT(System.currentTimeMillis()),
        POKE(System.currentTimeMillis());

        public long nexTick;

        EVENT(long nexTick) {
            this.nexTick = nexTick;
        }
    }

}
