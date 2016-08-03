package com.github.ibspoof.cassandraclient21;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.github.ibspoof.cassandraclient21.configs.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class App {

    private static Logger logger = LoggerFactory.getLogger(App.class.getSimpleName());

    private Session sessionLocal = null;
    private Session sessionRemote = null;

    private AppConfig config;

    private String localDc;
    private String remoteDc;


    App(Session sessionLocal, Session sessionRemote, AppConfig config) {
        this.sessionLocal = sessionLocal;
        this.sessionRemote = sessionRemote;
        this.config = config;

        this.localDc = config.getProperty("cassandra.datacenter.local");
        this.remoteDc = config.getProperty("cassandra.datacenter.remote");

        run();
    }


    private void run() {

        Statement query = new SimpleStatement("SELECT * FROM test LIMIT 10");
        query.setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);

        try {

            this.sessionLocal.execute(query);

        } catch (QueryExecutionException e) {

            logger.warn("CL = EACH_QUORUM failed, falling back to LOCAL_QUORUM w/ local DC");
            query.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            this.sessionLocal.execute(query);

        } catch (NoHostAvailableException e) {

            logger.warn("Local DC ({}) unable to be reached, falling back to remote ({}) w/ LOCAL_QUORUM", localDc, remoteDc);
            query.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            this.sessionRemote.execute(query);

        } catch (Exception e) {

            logger.error("Unable to run query to local or remote DC", e);

        }


    }


    private void queryLocal(SimpleStatement query) {

    }


}
