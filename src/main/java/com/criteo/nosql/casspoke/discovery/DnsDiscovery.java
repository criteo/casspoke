package com.criteo.nosql.casspoke.discovery;

import com.criteo.nosql.casspoke.config.Config;

import java.net.InetSocketAddress;
import java.util.*;

public class DnsDiscovery implements IDiscovery{

    private List<Config.DnsEntry> dnsEntries;

    public DnsDiscovery(List<Config.DnsEntry> dnsEntries) {
        this.dnsEntries = dnsEntries;

    }

    @Override
    public Map<Service, Set<InetSocketAddress>> getServicesNodes() {
        Map<Service, Set<InetSocketAddress>> clusters = new HashMap<>();

        for(Config.DnsEntry entry: dnsEntries) {
            Set<InetSocketAddress> addrs = new HashSet<>();
            for(String host: entry.getHost().split(",")) {
                String[] host_port = host.split(":");
                addrs.add(new InetSocketAddress(host_port[0], host_port.length > 1 ? Integer.parseInt(host_port[1]) : 9042));

            }
            List<String> tags = new ArrayList<>(); // not available for DnsDiscovery
            clusters.put(new Service(entry.getClustername(), tags), addrs);
        }
        return clusters;
    }

    @Override
    public void close() {

    }
}
