# Cassandra Exporter <img src="https://travis-ci.org/criteo/casspoke.svg?branch=master" alt="travis badge"/>

<p align="center">
  <img src="https://github.com/criteo/cassandra_exporter/raw/master/logo.png" alt="logo"/>
</p>

## Description

Casspoke is a blackbox probe measuring [Apache Cassandra®](http://cassandra.apache.org/) availibility and latency and exposing metrics throught an prometheus friendly endpoint.


## How to use

To start the application
> java -jar casspoke.jar config.yml

For the configuration take a loot at 
https://github.com/criteo/casspoke/blob/master/config.yml
There are 2 main sections: {app, discovery}

```yaml
app:
  # A measurement should be taken every x seconds
  measurementPeriodInSec: 10
  # Discovery of the cluster should be re-done every x seconds (usefull if you use consul or teardown often clusters)
  refreshDiscoveryPeriodInSec: 60
  # Prometheus http server port endpoint
  httpServerPort: 8080

discovery:
  staticDns:
    clustername: cstars04
    host: cassandra-seed1.fqdn:9042,cassandra-seed2.fqdn:9042
    
#  If you want to use consul to do the discovery    
#  consul:
#    host: consul.service.consul
#    port: 8500
#    timeoutInSec: 10
#    readConsistency: STALE
#    tags:
#      - cassandra

# Not usefull for now
service:
  type: CassandraRunner
  timeoutInSec: 60
```




