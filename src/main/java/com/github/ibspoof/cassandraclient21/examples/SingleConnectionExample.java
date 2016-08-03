package com.github.ibspoof.cassandraclient21.examples;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;

import java.util.concurrent.TimeUnit;

public class SingleConnectionExample {


    public SingleConnectionExample(String[] hosts, String username, String password) {

        /*
          Hosts should be in order of closest hosts (same DC) to farthest (remote DC).

          Max of 2-4 hosts hosts per DC are needed as the first one connected to will be used to
          determine all the clusters nodes and DCs
         */
        String[] hostList = hosts;

        /*
          The single line method is deprecated, the builder is 2.1.10+ method

          If usedHostsPerRemoteDc > 0, then if for a query no host in the local datacenter can be reached and if the consistency
          level of the query is not LOCAL_ONE or LOCAL_QUORUM, then up to usedHostsPerRemoteDc hosts per remote
          datacenter will be tried by the policy as a fallback.

          By default, no remote host will be used for LOCAL_ONE and LOCAL_QUORUM, since this would change the meaning
          of the consistency level, somewhat breaking the consistency contract
          (this can be overridden with allowRemoteDCsForLocalConsistencyLevel()).


          If allowRemoteDCsForLocalConsistencyLevel() is used it allows the policy to return remote hosts when building
          query plans for queries having consistency level LOCAL_ONE or LOCAL_QUORUM.

          When used in conjunction with usedHostsPerRemoteDc > 0, this overrides the policy of
          never using remote datacenter nodes for LOCAL_ONE and LOCAL_QUORUM queries. It is however inadvisable to do
          so in almost all cases, as this would potentially break consistency guarantees and if you are fine with that,
          it's probably better to use a weaker consistency like ONE, TWO or THREE. As such, this method should generally be
          avoided; use it only if you know and understand what you do.
         */
        TokenAwarePolicy tokenAwarePolicy = new TokenAwarePolicy(
                DCAwareRoundRobinPolicy.builder()
                        .withLocalDc("DC1")
                        .withUsedHostsPerRemoteDc(0)
                        .build()
        );


        /*
          Start reconnect at 500ms and exponentially increase to 5 mins before stopping
          Looks like: 500ms -> 1s -> 2s -> 4s -> 8s -> 16s -> 32s -> 1m4s -> 2m8s -> 4m16s -> stop
         */
        ExponentialReconnectionPolicy expReconnPolicy = new ExponentialReconnectionPolicy(500L, TimeUnit.MINUTES.toMillis(5));


        /*
          Set connection/read timeouts

          ConnectTimeoutMillis = 5seconds = default

          ReadTimeoutMillis =
          reduce this if needing to address lower SLAs than 12s, but expect higher number of timeouts
          it should be higher than the timeout settings used on the Cassandra side
         */
        SocketOptions socketOptions = new SocketOptions()
                .setConnectTimeoutMillis((int) TimeUnit.SECONDS.toMillis(5))
                .setReadTimeoutMillis((int) TimeUnit.SECONDS.toMillis(12));


        /*
          Set local DC to use min 1 connection (34k threads) up to 3 max

          Set remote DC to use min 1 and max 1 connection
          If you set DCAwareRoundRobinPolicy.withUsedHostsPerRemoteDc(0) then
          setConnectionsPerHost(HostDistance.REMOTE, 0, 0) can be used to limit the number of connections to a node
          opened.

          With a large number of application hosts (50+) this can reduce the amount of concurrent connections to a
          single C* node.
         */
        PoolingOptions poolingOptions = new PoolingOptions()
                .setConnectionsPerHost(HostDistance.LOCAL, 1, 3)
                .setConnectionsPerHost(HostDistance.REMOTE, 1, 1);


        /*
          DefaultRetryPolicy.INSTANCE:
           - onReadTimeout = retry query same host
           - onWriteTimeout = retry query same host
           - onUnavailable = tryNextHost
           - onConnectionTimeout = tryNextHost
         */
        LoggingRetryPolicy retryPolicy = new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE);


        /*
          Create connection

          Note: above code can be compressed to below, expanded to allow comments
         */
        Cluster cluster = Cluster.builder()
                .addContactPoints(hostList)
                .withCredentials(username, password)
                .withReconnectionPolicy(expReconnPolicy)
                .withRetryPolicy(retryPolicy)
                .withLoadBalancingPolicy(tokenAwarePolicy)
                .withPoolingOptions(poolingOptions)
                .withSocketOptions(socketOptions)
                .build();


        /*
          Create session/connect to all nodes with above settings
         */
        Session session = cluster.connect();
    }

}
