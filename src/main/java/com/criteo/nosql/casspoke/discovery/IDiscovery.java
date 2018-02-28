package com.criteo.nosql.casspoke.discovery;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IDiscovery extends AutoCloseable {

    Map<Service, Set<InetSocketAddress>> getServicesNodesFor();

    default void close() {
    }
}
