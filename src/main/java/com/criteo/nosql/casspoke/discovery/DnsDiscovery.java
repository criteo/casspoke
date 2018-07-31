package com.criteo.nosql.casspoke.discovery;

import java.net.InetSocketAddress;
import java.util.*;

public class DnsDiscovery implements IDiscovery{

    private String clusterName;
    private String hosts;

    public DnsDiscovery(String hosts, String clustername) {
        this.clusterName = clustername;
        this.hosts = hosts;

    }

    @Override
    public Map<Service, Set<InetSocketAddress>> getServicesNodes() {
        Set<InetSocketAddress> addrs = new HashSet<>();
        for(String host: hosts.split(",")) {
            String[] host_port = host.split(":");
            addrs.add(new InetSocketAddress(host_port[0], host_port.length > 1 ? Integer.parseInt(host_port[1]) : 9042));

        }
        return Collections.singletonMap(new Service(clusterName), addrs);
    }

    @Override
    public void close() {

    }
}
