package com.criteo.nosql.casspoke.cassandra;

import com.criteo.nosql.casspoke.config.Config;
import com.criteo.nosql.casspoke.consul.Consul;
import com.ecwid.consul.v1.health.model.HealthService;
import com.ecwid.consul.v1.health.model.HealthService.Node;
import com.ecwid.consul.v1.health.model.HealthService.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.text.html.Option;
import java.util.*;
import java.util.stream.Collectors;

public class CassandraRunner implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(CassandraRunner.class);
    private final Config cfg;

    // app
    private final long tickRate;
    // consul
    private final Consul consul;
    private final long refreshConsulInMs;
    private Map<Service, List<Node>> services;
    private Map<Service, Optional<CassandraMonitor>> monitors;
    private Map<Service, CassandraMetrics> metrics;

    public CassandraRunner(Config cfg) {
        this.cfg = cfg;
        Map<String, String> consulCfg = cfg.getConsul();

        this.tickRate = Long.parseLong(cfg.getApp().getOrDefault("tickRateInSec", "20")) * 1000L;
        this.refreshConsulInMs = Long.parseLong(consulCfg.getOrDefault("refreshEveryMin", "5")) * 60 * 1000L;

        this.consul = new Consul(consulCfg.get("host"), Integer.parseInt(consulCfg.get("port")),
                Integer.parseInt(consulCfg.get("timeoutInSec")), consulCfg.get("readConsistency"));
        this.services = Collections.emptyMap();
        this.monitors = Collections.emptyMap();
        this.metrics = Collections.emptyMap();
    }

    @Override
    public void run() {
        List<EVENT> evts = Arrays.asList(EVENT.UPDATE_TOPOLOGY, EVENT.WAIT, EVENT.POKE);
        EVENT evt;
        long start, stop;

        for(;;){
            start = System.currentTimeMillis();
            evt = evts.get(0);
            dispatch_events(evt);
            stop = System.currentTimeMillis();
            logger.info("{} took {} ms", evt, stop - start);

            resheduleEvent(evt, start, stop);
            Collections.sort(evts, Comparator.comparingLong(event -> event.nexTick));
        }
    }

    private enum EVENT {
        UPDATE_TOPOLOGY(System.currentTimeMillis()),
        WAIT(System.currentTimeMillis()),
        POKE(System.currentTimeMillis());

        public long nexTick;
        EVENT(long nexTick) {
            this.nexTick = nexTick;
        }
    }

    private void resheduleEvent(EVENT lastEvt, long start, long stop) {
        long duration  = stop - start;
        if(duration >= tickRate) {
            logger.warn("Operation took longer than 1 tick, please increase tick rate if you see this message too often");
        }

        EVENT.WAIT.nexTick = start + tickRate - 1;
        switch (lastEvt) {
            case WAIT:
                break;

            case UPDATE_TOPOLOGY:
                lastEvt.nexTick = start + refreshConsulInMs;
                break;

            case POKE:
                lastEvt.nexTick = start + tickRate;
                break;
        }
    }

    public void dispatch_events(EVENT evt) {
        switch (evt) {
            case WAIT:
                try {
                    Thread.sleep(Math.max(evt.nexTick - System.currentTimeMillis(), 0));
                } catch (Exception e) {logger.error("thread interrupted {}", e);}
                break;

            case UPDATE_TOPOLOGY:
                Map<HealthService.Service, List<HealthService.Node>> new_services = consul.getServicesNodesFor(cfg.getService().tags);

                // Consul down ?
                if(new_services.isEmpty()) {
                    logger.info("Consul sent back no services to monitor. is it down ? Are you sure of your tags ?");
                    break;
                }

                // Check if topology has changed
                if(Consul.areServicesEquals(services, new_services))
                    break;

                logger.info("Topology changed, updating it");
                // Clean old monitors
                monitors.values().forEach(mo -> mo.ifPresent(CassandraMonitor::close));
                metrics.values().forEach(CassandraMetrics::close);

                // Create new ones
                services = new_services;
                monitors = services.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> CassandraMonitor.fromNodes(e.getKey(), e.getValue(), cfg.getService().timeoutInSec * 1000L)));

                metrics = new HashMap<>(monitors.size());
                for(Map.Entry<HealthService.Service, Optional<CassandraMonitor>> client: monitors.entrySet()) {
                    metrics.put(client.getKey(), new CassandraMetrics(client.getKey()));
                }
                break;

            case POKE:
                monitors.entrySet().parallelStream().forEach(client -> {
                    CassandraMetrics m = metrics.get(client.getKey());
                    m.updateAvailability(client.getValue().map(CassandraMonitor::collectAvailability).orElse(Collections.EMPTY_MAP));
                });
                break;
        }
    }

}
