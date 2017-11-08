package com.criteo.nosql.casspoke.consul;

import com.ecwid.consul.v1.ConsistencyMode;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.health.model.HealthService.Node;
import com.ecwid.consul.v1.health.model.HealthService.Service;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;

public class Consul implements AutoCloseable {
    Logger logger = LoggerFactory.getLogger(Consul.class);

    private final String host;
    private final int port;
    private final int timeout;
    private final QueryParams params;
    private final ExecutorService executor;
    private static final String MAINTENANCE_MODE = "_node_maintenance";

    public Consul(String host, int port, int timeout, String readConsistency) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.params = new QueryParams(ConsistencyMode.valueOf(readConsistency));
        this.executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("consul-%d").build());
    }

    public static boolean areServicesEquals(final Map<Service, List<Node>> ori, Map<Service, List<Node>> neo) {

        //TODO Improve complexity of the funtion
        if(ori.size() != neo.size())
            return false;

        //TODO Replace the toString by a real comparator
        List<String> ori_nodes = ori.entrySet().stream().flatMap(service ->  service.getValue().stream().map(n -> service.getKey().getId() +  service.getKey().getPort() + n.getAddress())).collect(toList());
        List<String> neo_nodes = neo.entrySet().stream().flatMap(service ->  service.getValue().stream().map(n -> service.getKey().getId() +  service.getKey().getPort() + n.getAddress())).collect(toList());
        if(neo_nodes.size() != ori_nodes.size())
            return false;

        neo_nodes.retainAll(ori_nodes);
        if(neo_nodes.size() != ori_nodes.size())
            return false;

        return true;
    }

    public static String getClusterName(Service service) {
        final String clusterPrefix = "cluster=";

        return service.getTags().stream()
                .filter(tag -> tag.startsWith(clusterPrefix))
                .map(tag -> tag.substring(clusterPrefix.length()))
                .findFirst().orElse("NotDefined");
    }

    public static String getBucketName(Service service) {
        final String bucketPrefix = "bucket=";

        return service.getTags().stream()
                .filter(tag -> tag.startsWith(bucketPrefix))
                .map(tag -> tag.substring(bucketPrefix.length()))
                .findFirst().orElse("NotDefined");
    }

    private Map<Service, List<Node>> getServicesNodesForImpl(List<String> tags) {
        ConsulClient client = new ConsulClient(host, port);

        List<Map.Entry<String, List<String>>> services = client.getCatalogServices(params).getValue().entrySet().stream()
                .filter(entry -> !Collections.disjoint(entry.getValue(), tags))
                .map(entry -> {
                    logger.info("Found service matching {}", entry.getKey());
                    return entry;
                })
                .collect(toList());

        Map<Service, List<Node>> servicesNodes = new HashMap<>(services.size());
        final Service[] service = new Service[1];
        for(Map.Entry<String, List<String>> serviceE: services) {
            service[0] = null;
            final List<Node> nodes = new ArrayList<>();
            client.getHealthServices(serviceE.getKey(), false, params).getValue().stream()
                    .filter(srv -> srv.getChecks().stream().noneMatch(check -> check.getCheckId().equalsIgnoreCase(MAINTENANCE_MODE)
                            || check.getOutput().startsWith("DISCARD:") // For couchbase, flaky but don't have better, come propose me better
                            || (check.getCheckId().startsWith("service:couchbase") && check.getOutput().isEmpty())
                    ))
                    .forEach(srv -> {
                        logger.debug("{}", srv.getNode());
                        service[0] = srv.getService();
                        nodes.add(srv.getNode());
                    });
            if (service[0] != null) {
                servicesNodes.put(service[0], nodes);
            }
        }
        return servicesNodes;

    }

    /**
     * Entry point of the class, that fetch all services with associated nodes matching the given tag
     * The function filter out nodes that are in maintenance mode
     * @param tags
     * @return
     *
     * All this mumbo-jumbo with the executor is done only because the consul client does not expose
     * in any way a mean to timeout/cancel requests nor to properly shutdown/reset it.
     * Thus we play safe and wrap calls inside an executor that we can properly timeout, and a new consul client
     * is created each time.
     */
    public Map<Service, List<Node>> getServicesNodesFor(List<String> tags) {
        Future<Map<Service, List<Node>>> fServices = null;
        try {
            fServices = executor.submit(() -> {
                logger.info("Fetching services for tag {} ", tags);
                long start = System.currentTimeMillis();

                Map<Service, List<Node>> services = getServicesNodesForImpl(tags);

                long stop = System.currentTimeMillis();
                logger.info("Fetching services for tag {} took {} ms", tags, stop - start);
                return services;
            });
            return fServices.get(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Cannot fetch nodes for tag {}", tags, e);

            if(fServices != null) {
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
