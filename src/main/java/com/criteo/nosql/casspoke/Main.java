package com.criteo.nosql.casspoke;

import com.criteo.nosql.casspoke.cassandra.CassandraRunner;
import com.criteo.nosql.casspoke.config.Config;
import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class Main {

    static private Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Optional<Config> cfgO = Config.fromFile(args.length > 0 ? args[0] : Config.DEFAULT_PATH);
        if (!cfgO.isPresent()) return;

        Config cfg = cfgO.get();
        HTTPServer server = new HTTPServer(Integer.parseInt(cfg.getApp().getOrDefault("httpServerPort", "8080")));

        for(;;) {
            try {
                new CassandraRunner(cfg).run();
            } catch (Throwable e) {
                logger.error("Uncaught Exception", e);
            }
        }
    }

}
