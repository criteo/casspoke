package com.criteo.nosql.casspoke;

import com.criteo.nosql.casspoke.cassandra.CassandraRunner;
import com.criteo.nosql.casspoke.config.Config;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class MainIT {

    //@Test
    public void test() throws IOException, InterruptedException {

        final Config cfg = Config.fromFile(Config.DEFAULT_PATH);
        final CassandraRunner runner = new CassandraRunner(cfg);
        runner.run();

        HashMap<InetSocketAddress, Long> latencies = new HashMap<>();
        latencies.put(InetSocketAddress.createUnresolved("toto", 8080), 50L);
        TimeUnit.SECONDS.sleep(60);
    }
}
