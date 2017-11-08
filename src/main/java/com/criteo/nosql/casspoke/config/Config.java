package com.criteo.nosql.casspoke.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Configuration class that are use by the app
 * Use fromFile method in order to transform a yaml file into an config's instance
 *
 * TODO (r.gerard): Improve the class, add substructure for consul and services instead of Map<String,String>
 */
public final class Config {

    public static final String DEFAULT_PATH = "config.yml";

    public static Optional<Config> fromFile(String filePath) {
        Logger logger = LoggerFactory.getLogger(Config.class);
        logger.info("Loading yaml config from {}", filePath);

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            Config cfg = mapper.readValue(new File(filePath), Config.class);
            logger.trace(cfg.toString());
            return Optional.of(cfg);
        } catch (Exception e) {
            logger.error("Cannot load config file", e);
            return Optional.empty();
        }
    }

    private Map<String, String> app;
    private Map<String, String> consul;
    private Services service;

    public Services getService() {
        return service;
    }

    public void setService(Services service) {
        this.service = service;
    }


    public class Services {
        public List<String> tags;
        public long timeoutInSec;
        public String type;
        public String username;
        public String password;
    }
    public Map<String, String> getConsul() {
        return consul;
    }

    public void setConsul(Map<String, String> consul) {
        this.consul = consul;
    }

    public Map<String, String> getApp() {
        return app;
    }

    public void setApp(Map<String, String> app) {
        this.app = app;
    }
}

