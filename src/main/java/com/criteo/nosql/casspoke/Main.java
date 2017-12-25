package com.criteo.nosql.casspoke;

import com.criteo.nosql.casspoke.cassandra.CassandraRunnerLatency;
import com.criteo.nosql.casspoke.cassandra.CassandraRunnerStats;
import com.criteo.nosql.casspoke.config.Config;
import com.criteo.nosql.casspoke.discovery.Consul;
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

        // Get Consul discovery
        final IDiscovery consulDiscovery;
        try {
            final Map<String, String> consulCfg = cfg.getConsul();
            logger.info("Connecting to consul with config {}", consulCfg);
            consulDiscovery = Consul.fromConfig(consulCfg);
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
        final String serviceType = cfg.getService().type;
        logger.info("Loading service {}", serviceType);
        final Runnable runner;
        try {
            switch (serviceType) {
                case "CassandraRunnerStats":
                    runner = new CassandraRunnerStats(cfg, consulDiscovery);
                    break;
                case "CassandraRunnerLatency":
                    runner = new CassandraRunnerLatency(cfg, consulDiscovery);
                    break;
                default:
                    final Class clazz = Class.forName(serviceType);
                    runner = (Runnable) clazz
                            .getConstructor(Config.class, IDiscovery.class)
                            .newInstance(cfg, consulDiscovery);
                    break;
            }
        } catch (Exception e){
            logger.error("Cannot load the service {}", serviceType, e);
            return;
        }

        logger.info("Run {}", serviceType);
        for (; ; ) {
            try {
                runner.run();
            } catch (Exception e) {
                logger.error("An unexpected exception was thrown", e);
                logger.info("Run {} again", serviceType);
            } catch (Error e) {
                logger.error("An unexpected error was thrown, that indicates serious problems. The program will exit", e);
                consulDiscovery.close();
                throw e;
            }
        }
    }
}