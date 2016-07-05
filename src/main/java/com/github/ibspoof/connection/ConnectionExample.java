package com.github.ibspoof.connection;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;

import java.util.concurrent.TimeUnit;

public class ConnectionExample {


    public ConnectionExample(String[] hosts, String username, String password) {

        // Hosts should be in order of closest hosts (same DC) to farthest (remote DC) 2-4 hosts max per DC
        String[] hostList = hosts;

        // the single line method is deprecated, the builder is 2.1.10+method
        TokenAwarePolicy tokenAwarePolicy = new TokenAwarePolicy(
                DCAwareRoundRobinPolicy.builder()
                        .withLocalDc("DC1")
                        .build()
        );

        // Start reconnect at 500ms and exponentially increase to 2 mins before stopping
        // Looks like: 500ms -> 1s -> 2s -> 4s -> 8s -> 16s -> 32s -> 1m4s -> 2m8s -> 4m16s -> stop
        ExponentialReconnectionPolicy expReconnPolicy = new ExponentialReconnectionPolicy(500L, TimeUnit.MINUTES.toMillis(5));


        // Set connection/read timeouts
        SocketOptions socketOptions = new SocketOptions()
                .setConnectTimeoutMillis(5000)  // 5seconds = default
                .setReadTimeoutMillis(10000);   // reduce this if needing to address lower SLAs than 10s, but expect higher number of timeouts


        // Set local DC to use min 1 connection (34k threads) up to 3 max
        // Set remote DC to use min 1 and max 1 connection
        PoolingOptions poolingOptions = new PoolingOptions()
                .setConnectionsPerHost(HostDistance.LOCAL, 1, 3)
                .setConnectionsPerHost(HostDistance.REMOTE, 1, 1);


        // DefaultRetryPolicy.INSTANCE:
        // onReadTimeout = retry query same host
        // onWriteTimeout = retry query same host
        // onUnavailable = tryNextHost
        // onConnectionTimeout = tryNextHost
        LoggingRetryPolicy retryPolicy = new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE);


        // Create connection
        // Note: above can be compressed, expanded to allow comments
        Cluster cluster = Cluster.builder()
                .addContactPoints(hostList)
                .withCredentials(username, password)
                .withReconnectionPolicy(expReconnPolicy)
                .withRetryPolicy(retryPolicy)
                .withLoadBalancingPolicy(tokenAwarePolicy)
                .withPoolingOptions(poolingOptions)
                .withSocketOptions(socketOptions)
                .build();


        Session session = cluster.connect();
    }

}
