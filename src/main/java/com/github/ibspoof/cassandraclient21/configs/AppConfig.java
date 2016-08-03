package com.github.ibspoof.cassandraclient21.configs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class AppConfig {

    private InputStream inputStream;
    private final String propFileName = "application.properties";
    private Properties prop = new Properties();
    private static final Logger logger = LoggerFactory.getLogger(AppConfig.class.getSimpleName());

    public AppConfig() throws IOException {

        try {

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

        } catch (Exception e) {
            logger.error("Exception while loading " + propFileName + " " + e);
            System.exit(1);
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

    public Properties getAllProperties() {
        return prop;
    }

    public String getProperty(String path) {
        logger.debug("Getting property path {}", path);
        return prop.getProperty(path);
    }

}
