package com.criteo.nosql.casspoke.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Configuration class that are use by the app
 * Use fromFile method in order to transform a yaml file into an config's instance
 * <p>
 * TODO (r.gerard): Improve the class, add substructure for consul and services instead of Map<String,String>
 */
public final class Config {

    public static final String DEFAULT_PATH = "config.yml";
    private Map<String, String> app;
    private Map<String, String> consul;
    private Services service;

    public static Config fromFile(String filePath) throws IOException {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(filePath), Config.class);
    }

    public Services getService() {
        return service;
    }

    public void setService(Services service) {
        this.service = service;
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

    public class Services {
        public List<String> tags;
        public long timeoutInSec;
        public String type;
        public String username;
        public String password;
    }
}

