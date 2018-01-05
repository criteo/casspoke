package com.criteo.nosql.casspoke.discovery;

public class Service {
    private final String clusterName;

    public Service(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterName() {
        return clusterName;
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
