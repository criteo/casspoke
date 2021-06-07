package com.criteo.nosql.casspoke.discovery;

import java.util.List;

public class Service {
    private final String clusterName;
    private final List<String> tags;

    public Service(String clusterName, List<String> tags) {
        this.clusterName = clusterName;
        this.tags = tags;
    }

    public String getClusterName() {
        return clusterName;
    }

    public List<String> getTags() {
        return tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Service service = (Service) o;

        return clusterName != null ? clusterName.equals(service.clusterName) : service.clusterName == null;
    }

    @Override
    public int hashCode() {
        return clusterName != null ? clusterName.hashCode() : 0;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("Service {cluster='")
                .append(clusterName)
                .append("'}")
                .toString();
    }
}
