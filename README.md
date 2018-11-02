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




