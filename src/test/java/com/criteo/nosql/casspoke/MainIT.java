package com.criteo.nosql.casspoke;

import com.criteo.nosql.casspoke.cassandra.CassandraRunner;
import com.criteo.nosql.casspoke.config.Config;
import com.criteo.nosql.casspoke.discovery.ConsulDiscovery;

import com.criteo.nosql.casspoke.discovery.IDiscovery;
import com.criteo.nosql.casspoke.utils.MetricsFetcher;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MainIT {

    private static final Logger logger = LoggerFactory.getLogger(MainIT.class);

    @Before
    public void cleanup() throws NoSuchFieldException, IllegalAccessException, IOException {
        final Config cfg = Config.fromFile(Config.DEFAULT_PATH);
        final MetricsFetcher metricsFetcher = new MetricsFetcher(cfg);

        metricsFetcher.getUp().clear();
    }

    // TODO: Discovery and CassandraMetrics should be mocked:
    // TODO: - to not declare stuff public just for tests
    // TODO: - to have reliable unit tests that not depend on the environment
    //@Test
    public void testStats() throws IOException, NoSuchFieldException, IllegalAccessException {

        final Config cfg = Config.fromFile(Config.DEFAULT_PATH);
        final MetricsFetcher metricsFetcher = new MetricsFetcher(cfg);
        final IDiscovery discovery = new ConsulDiscovery(cfg.getDiscovery().getConsul());
        final CassandraRunner runner = new CassandraRunner(cfg, discovery);

        Assert.assertFalse("'UP' prometheus gauge should have been initialized", metricsFetcher.getUp().collect().isEmpty());
        Assert.assertTrue("'UP' prometheus gauge should be empty", metricsFetcher.getUp().collect().get(0).samples.isEmpty());
        runner.updateTopology();
        runner.poke();
        Assert.assertFalse("'UP' prometheus gauge should not be empty after a poke", metricsFetcher.getUp().collect().get(0).samples.isEmpty());
        logger.info("UP samples: {}", metricsFetcher.getUp().collect().get(0).samples);
    }

    // TODO: Discovery and CassandraMetrics should be mocked:
    // TODO: - to not declare stuff public just for tests
    // TODO: - to have reliable unit tests that not depend on the environment
    //@Test
    public void testLatency() throws IOException, NoSuchFieldException, IllegalAccessException {

        final Config cfg = Config.fromFile(Config.DEFAULT_PATH);
        final MetricsFetcher metricsFetcher = new MetricsFetcher(cfg);
        final IDiscovery discovery = new ConsulDiscovery(cfg.getDiscovery().getConsul());
        final CassandraRunner runner = new CassandraRunner(cfg, discovery);

        Assert.assertFalse("'LATENCY' prometheus gauge should have been initialized", metricsFetcher.getLatency().collect().isEmpty());
        Assert.assertTrue("'LATENCY' prometheus gauge should be empty", metricsFetcher.getLatency().collect().get(0).samples.isEmpty());
        runner.updateTopology();
        runner.poke();
        Assert.assertFalse("'LATENCY' prometheus gauge should not be empty after a poke", metricsFetcher.getLatency().collect().get(0).samples.isEmpty());
        logger.info("LATENCY samples: {}", metricsFetcher.getLatency().collect().get(0).samples);
    }
}
