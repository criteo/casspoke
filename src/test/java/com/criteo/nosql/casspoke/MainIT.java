package com.criteo.nosql.casspoke;

import com.criteo.nosql.casspoke.cassandra.CassandraRunnerStats;
import com.criteo.nosql.casspoke.config.Config;
import com.criteo.nosql.casspoke.discovery.Consul;

import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class MainIT {

    //@Test
    public void test() throws IOException, InterruptedException {

        final Config cfg = Config.fromFile(Config.DEFAULT_PATH);
        final Consul consulDiscovery = Consul.fromConfig(cfg.getConsul());
        final long refreshDiscoveryInMs= Long.parseLong(cfg.getConsul().getOrDefault("refreshEveryMin", "5")) * 60L * 1000L;

        final Runnable runner = new CassandraRunnerStats(cfg, consulDiscovery, refreshDiscoveryInMs);
        runner.run();

        //HashMap<InetSocketAddress, Long> latencies = new HashMap<>();
        //latencies.put(InetSocketAddress.createUnresolved("toto", 8080), 50L);
        //TimeUnit.SECONDS.sleep(60);
    }
}
