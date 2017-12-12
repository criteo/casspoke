package com.criteo.nosql.casspoke;

import com.criteo.nosql.casspoke.cassandra.CassandraMetrics;
import com.criteo.nosql.casspoke.cassandra.CassandraRunnerStats;
import com.criteo.nosql.casspoke.config.Config;
import com.criteo.nosql.casspoke.discovery.Consul;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MainIT {

    private static final Logger logger = LoggerFactory.getLogger(MainIT.class);

    @Before
    public void cleanup() {
        CassandraMetrics.UP.clear();
    }

    // TODO: Discovery and CassandraMetrics should be mocked:
    // TODO: - to not declare stuff public just for tests
    // TODO: - to have reliable unit tests that not depend on the environment
    //@Test
    public void testStats() throws IOException, InterruptedException {

        final Config cfg = Config.fromFile(Config.DEFAULT_PATH);
        final Consul consulDiscovery = Consul.fromConfig(cfg.getConsul());
        final long refreshDiscoveryInMs= Long.parseLong(cfg.getConsul().getOrDefault("refreshEveryMin", "5")) * 60L * 1000L;
        final CassandraRunnerStats runner = new CassandraRunnerStats(cfg, consulDiscovery, refreshDiscoveryInMs);

        Assert.assertFalse("'UP' prometheus gauge is initialized", CassandraMetrics.UP.collect().isEmpty());
        Assert.assertTrue("'UP' prometheus gauge is empty", CassandraMetrics.UP.collect().get(0).samples.isEmpty());
        runner.updateTopology();
        runner.poke();
        Assert.assertFalse("'UP' prometheus gauge is not empty after a poke", CassandraMetrics.UP.collect().get(0).samples.isEmpty());
        logger.info("UP samples: {}", CassandraMetrics.UP.collect().get(0).samples);
    }
}
