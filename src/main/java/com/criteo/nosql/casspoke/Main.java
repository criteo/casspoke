package com.criteo.nosql.casspoke;

import com.criteo.nosql.casspoke.cassandra.CassandraRunnerLatency;
import com.criteo.nosql.casspoke.cassandra.CassandraRunnerStats;
import com.criteo.nosql.casspoke.config.Config;
import com.criteo.nosql.casspoke.discovery.ConsulDiscovery;
import com.criteo.nosql.casspoke.discovery.IDiscovery;
import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

        // Get the configuration
        final Config cfg;
        try {
            final String cfgPath = args.length > 0 ? args[0] : Config.DEFAULT_PATH;
            logger.info("Loading yaml config from {}", cfgPath);
            cfg = Config.fromFile(cfgPath);
            logger.trace("Loaded configuration: {}", cfg);
        } catch (Exception e){
            logger.error("Cannot load config file", e);
            return;
        }

        // Get the discovery
        final IDiscovery discovery;
        try {
            final Map<String, String> consulCfg = cfg.getConsul();
            logger.info("Connecting to consul with config {}", consulCfg);
            discovery = ConsulDiscovery.fromConfig(consulCfg);
        } catch (Exception e){
            logger.error("Cannot connect to Consul", e);
            return;
        }

        // Start an http server to allow Prometheus scrapping
        // daemon=true, so if the scheduler is stopped, the JVM does not wait for http server termination
        final int httpServerPort = Integer.parseInt(cfg.getApp().getOrDefault("httpServerPort", "8080"));
        logger.info("Starting an http server on port {}", httpServerPort);
        final HTTPServer server = new HTTPServer(httpServerPort, true);

        // Get the runner depending on the configuration
        final String runnerType = cfg.getService().type;

        // If an unexpected exception occurs, we retry
        for (; ; ) {
            logger.info("Loading runner {}", runnerType);
            try(AutoCloseable runner = getRunner(runnerType, cfg, discovery)){
                try {
                    logger.info("Run {}", runnerType);
                    ((Runnable)runner).run();
                } catch (Exception e) {
                    logger.error("An unexpected exception was caught. We will rerun {}", runnerType, e);
                } catch (Error e) {
                    logger.error("An unexpected error was thrown, that indicates serious problems. The program will exit", e);
                    discovery.close();
                    server.stop();
                    throw e;
                }
            }
        }
    }

    private static AutoCloseable getRunner(String type, Config cfg, IDiscovery discovery) {
        try {
            switch (type) {
                case "CassandraRunnerStats":
                    return new CassandraRunnerStats(cfg, discovery);
                case "CassandraRunnerLatency":
                    return new CassandraRunnerLatency(cfg, discovery);
                default:
                    final Class clazz = Class.forName(type);
                    return (AutoCloseable) clazz
                            .getConstructor(Config.class, IDiscovery.class)
                            .newInstance(cfg, discovery);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot load the service " + type, e);
        }
    }
}