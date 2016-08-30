## Cassandra Java 2.1.x Client Example w/ Fallback CL and DC

The [DataStax C* driver](https://github.com/datastax/java-driver) can hide it's fallback behavior from the application side even with logging enabled. This project was created to give and example of how to use two connections for local and remote DCs and catch connection or CL exceptions to fallback to both a lower CL or remoteDC.  

### Main Application
`/src/main/java/com/github/ibspoof/connection/App.run()` function contains the fallback steps which are:

1. Write to localDC w/ EACH_QUORUM
2. Fallback to localDC w/ LOCAL_QUORUM
3. Fallback to remoteDC w/ LOCAL_QUORUM

Each of the above is logged to enable tracking of the codes decision tree.
 
### Single Connection Example
Additionally a fully documented Single connection example is included in the `/src/main/java/com/github/ibspoof/connection/examples` package.``