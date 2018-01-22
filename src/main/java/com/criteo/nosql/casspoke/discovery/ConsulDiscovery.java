package com.criteo.nosql.casspoke.discovery;

import com.ecwid.consul.v1.ConsistencyMode;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.health.model.HealthService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;

/**
 * Provide a discovery based on Consul.
 */
public class ConsulDiscovery implements IDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(ConsulDiscovery.class);

    private static final String MAINTENANCE_MODE = "_node_maintenance";

    private final String host;
    private final int port;
    private final int timeout;
    private final QueryParams params;
    private final ExecutorService executor;

    public ConsulDiscovery(final String host, final int port, final int timeout, final String readConsistency) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.params = new QueryParams(ConsistencyMode.valueOf(readConsistency));
        this.executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("consul-%d").build());
    }

    public static ConsulDiscovery fromConfig(final Map<String, String> consulCfg) {
        final String host = consulCfg.get("host");
        final int port = Integer.parseInt(consulCfg.getOrDefault("port", "8500"));
        final int timeout = Integer.parseInt(consulCfg.getOrDefault("timeoutInSec", "10"));
        final String readConsistency = consulCfg.getOrDefault("readConsistency", "STALE");
        return new ConsulDiscovery(host, port, timeout, readConsistency);
    }

    private static String getFromTags(final HealthService.Service service, final String prefix) {
        return service.getTags().stream()
                .filter(tag -> tag.startsWith(prefix))
                .map(tag -> tag.substring(prefix.length()))
                .findFirst().orElse("NotDefined");
    }

    public static String getClusterName(final HealthService.Service service) {
        return getFromTags(service, "cluster-");
    }

    private Map<Service, Set<InetSocketAddress>> getServicesNodesForImpl(final List<String> tags) {
        final ConsulClient client = new ConsulClient(host, port);

        final List<Map.Entry<String, List<String>>> services =
                client.getCatalogServices(params).getValue().entrySet().stream()
                        .filter(entry -> !Collections.disjoint(entry.getValue(), tags))
                        .map(entry -> {
                            logger.info("Found service matching {}", entry.getKey());
                            return entry;
                        })
                        .collect(toList());

        final Map<Service, Set<InetSocketAddress>> servicesNodes = new HashMap<>(services.size());
        for (Map.Entry<String, List<String>> service : services) {
            final Set<InetSocketAddress> nodes = new HashSet<>();
            final Service[] srv = new Service[]{null};
            client.getHealthServices(service.getKey(), false, params).getValue().stream()
                    .filter(hsrv -> hsrv.getChecks().stream()
                            .noneMatch(check -> check.getCheckId().equalsIgnoreCase(MAINTENANCE_MODE))
                    )
                    .forEach(hsrv -> {
                        logger.debug("{}", hsrv.getNode());
                        nodes.add(new InetSocketAddress(hsrv.getNode().getAddress(), hsrv.getService().getPort()));
                        srv[0] = new Service(getClusterName(hsrv.getService()));
                    });
            if (nodes.size() > 0) {
                servicesNodes.put(srv[0], nodes);
            }
        }
        return servicesNodes;
    }

    /**
     * Look in Consul for all services matching one of the tags
     * The function filter out nodes that are in maintenance mode
     *
     * @param tags
     * @return the map nodes by services
     */
    // All this mumbo-jumbo with the executor is done only because the consul client does not expose
    // in any way a mean to timeout/cancel requests nor to properly shutdown/reset it.
    // Thus we play safe and wrap calls inside an executor that we can properly timeout, and a new consul client
    // is created each time.
    public Map<Service, Set<InetSocketAddress>> getServicesNodesFor(final List<String> tags) {
        Future<Map<Service, Set<InetSocketAddress>>> fServices = null;
        try {
            fServices = executor.submit(() -> {
                logger.info("Fetching services for tag {} ", tags);
                final long start = System.currentTimeMillis();
                final Map<Service, Set<InetSocketAddress>> services = getServicesNodesForImpl(tags);
                final long stop = System.currentTimeMillis();
                logger.info("Fetching services for tag {} took {} ms", tags, stop - start);
                return services;
            });
            return fServices.get(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Cannot fetch nodes for tag {}", tags, e);

            if (fServices != null) {
                fServices.cancel(true);
            }
            return Collections.emptyMap();
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
