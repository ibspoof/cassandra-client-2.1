package com.github.ibspoof.cassandraclient21;

import com.datastax.driver.core.Session;
import com.github.ibspoof.cassandraclient21.cassandra.CassandraSession;
import com.github.ibspoof.cassandraclient21.configs.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Launcher {

    private static final Logger logger = LoggerFactory.getLogger(Launcher.class.getSimpleName());
    private static Session sessionLocal;
    private static Session sessionRemote;
    private static AppConfig config;

    public static void main(String... args) {

        logger.info("Starting application example...");

        try {
            config = new AppConfig();
        } catch(Exception e) {
            logger.error("Unable to load configs", e);
            System.exit(1);
        }

        String localDc = config.getProperty("cassandra.datacenter.local");
        String remoteDc = config.getProperty("cassandra.datacenter.remote");

        logger.info("Local dc = {}", localDc);
        logger.info("Remote dc = {}", remoteDc);

        CassandraSession cassSession = new CassandraSession(config);

        sessionLocal = cassSession.getSession(localDc);
        sessionRemote = cassSession.getSession(remoteDc);

        if (sessionLocal == null && sessionRemote == null) {
            logger.error("Unable to connect to either DSE DC. Exiting.");
            System.exit(1);
        }

        new App(sessionLocal,sessionRemote, config);

        System.exit(1);
    }

}
