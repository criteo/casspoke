# Cassandra Exporter <img src="https://travis-ci.org/criteo/casspoke.svg?branch=master" alt="travis badge"/>

<p align="center">
  <img src="https://github.com/criteo/cassandra_exporter/raw/master/logo.png" alt="logo"/>
</p>

## Description

Casspoke is a blackbox probe measuring [Apache Cassandra®](http://cassandra.apache.org/) availibility and latency. It exposes metrics throught an prometheus friendly endpoint.
The probe will connect to cassandra and:
  - Listen for topology change (like a node changing state from UN to DN) and expose the availibility throught metric
  - Send periodically to each node write and read requests to the `system_traces.events` table in order to measure latency to this node
  
## How to use

To start the application
> java -jar casspoke.jar config.yml

The prometheus endpoint is available at http://localhost:{httpServerPort} (8080 by default)

For the configuration take a loot at 
https://github.com/criteo/casspoke/blob/master/config.yml

There are 2 main sections: {app, discovery}

```yaml
app:
  # A measurement should be taken every x seconds
  measurementPeriodInSec: 10
  # Operations timeout
  timeoutInSec: 60
  # Discovery of the cluster should be re-done every x seconds (useful if you use consul)
  refreshDiscoveryPeriodInSec: 600
  # Prometheus http server port endpoint
  httpServerPort: 8080
  # Optional: username used to auth to cassandra
  # username: user
  # Optional: password used to auth to cassandra
  # password: password
  # Optional: probe location to see which data center or cloud have problems with connection to cassandra
  # probeLocation: myGreatDataCenter
  # Optional: label prefix, change it to avoid collisions with other labels, appended to metric, by Prometheus server
  # labelPrefix: cassandra

discovery:
  dns:
    - clustername: cstars01
      host: cstars01-seed1.fqdn:9042,cstars01-seed2.fqdn:9042
    - clustername: cstars02
      host: cstars02-seed1.fqdn:9042,cstars02-seed2.fqdn:9042
#  consul:
#    host: consul.service.consul
#    port: 8500
#    timeoutInSec: 10
#    readConsistency: STALE
#    tags:
#      - cluster=cstars01
#      #- cassandra

```

## How to build
At the root of the project, just do a
> gradle build

you will find the jar in build/libs directory

## Docker
> docker run --rm --net=host -v $(pwd)/config.yml:/opt/casspoke/config.yml criteord/casspoke:1.2 



