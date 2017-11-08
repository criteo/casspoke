package com.criteo.nosql.casspoke.httpserver;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HTTPServer implements AutoCloseable {

    private ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("httpserver-%d").build());

    @Override
    public void close() throws Exception {
        executor.shutdownNow();
    }

    static class HTTPMetricHandler implements HttpHandler {
        private ThreadLocal<ByteArrayOutputStream> responseBuffer = ThreadLocal.withInitial(() -> new ByteArrayOutputStream(1 << 20));

        public void handle(HttpExchange t) throws IOException {
            ByteArrayOutputStream response = dump();
            t.getResponseHeaders().set("Content-Type", TextFormat.CONTENT_TYPE_004);
            t.getResponseHeaders().set("Content-Length", String.valueOf(response.size()));
            t.sendResponseHeaders(200, response.size());
            response.writeTo(t.getResponseBody());
            t.close();
        }

        private ByteArrayOutputStream dump() throws java.io.IOException {
            // TODO: try to reuse some byteArray :(
            try(ByteArrayOutputStream response = responseBuffer.get();
                OutputStreamWriter osw = new OutputStreamWriter(response)) {

                response.reset();
                TextFormat.write004(osw, CollectorRegistry.defaultRegistry.metricFamilySamples());
                response.flush();
                osw.flush();
                return response;
            }
        }
    }

    public HTTPServer(int port) throws java.io.IOException {
        HttpServer mServer = HttpServer.create();
        mServer.bind(new java.net.InetSocketAddress(port), 4);
        HttpHandler mHandler = new HTTPMetricHandler();
        mServer.createContext("/", mHandler);
        mServer.createContext("/metrics", mHandler);
        mServer.setExecutor(executor);
        mServer.start();
    }
}
