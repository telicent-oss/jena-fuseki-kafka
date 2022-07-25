# jena-fuseki-kafka 

Apache Jena Fuseki extension module for receiving data over
Apache Kafka topics.

License: Apache License 2.0.  
See [LICENSE](./LICENSE).

The Fuseki-Kafka connector receives Kafka events on a topic and applies them to
a Fuseki server. In effect, it is the same as if an HTTP POST request is sent to
the Fuseki service.

The Kafka event must have a Kafka message header "Content-Type" set to the MIME
type of the content. No other headers are required.

SPARQL Update is supported.
RDF data is added into the dataset of the service.

The Fuseki service must be configured with the "update" if SPARQL Update request
are sent, and "gsp-rw" for RDF data (any syntax supported by Jena).

```
:service rdf:type fuseki:Service ;
    fuseki:name "ds" ;
    fuseki:endpoint [ fuseki:operation fuseki:query ] ;
    ...

    # Fuseki-Kafka
    fuseki:endpoint [ fuseki:operation fuseki:update ] ;
    fuseki:endpoint [ fuseki:operation fuseki:gsp-rw ] ;

    ...
    fuseki:dataset ... ;
    .
```

See "[Configuring Fuseki](https://jena.apache.org/documentation/fuseki2/fuseki-configuration.html)"
for access control and other features.

This project uses the Apache Jena Fuseki Main server and is configured with a
Fuseki configuration file.

## Connector Configuration

The connector configuration goes in the Fuseki configuration file.

```
PREFIX :        <#>
PREFIX fuseki:  <http://jena.apache.org/fuseki#>
PREFIX fk:      <http://jena.apache.org/fuseki/kafka#>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ja:      <http://jena.hpl.hp.com/2005/11/Assembler#>

<#connector> rdf:type fk:Connector ;
    # Kafka topic
    fk:topic              "RDF";

    # Destination Fuseki service. This is a URI path (no scheme, host or port).
    # This can be the dataset, a specific endpoint ("/ds/kafkaIncoming")
    # with the necessary fuseki:operation.
    
    fk:fusekiServiceName  "/ds";

    # Using Kafka-RAFT
    fk:bootstrapServers   "localhost:9092";

    # File used to track the state (the last offset processes)
    # Used across Fuseki restarts.
    fk:stateFile        "Databases/RDF.state";

    # Kafka GroupId - default "JenaFusekiKafka"
    # fk:groupId          "JenaFusekiKafka";

    # What to do on start up.
    # Normally this is "sync" which is the default value.
    # If replay is true, then on start-up the whole topic is replayed.
    #    fk:replayTopic      true;
    #    fk:syncTopic        true;

## Additional Kafka client properties.
##   fk:config ( "key" "value") ;
    .
```

### Build

Run
```
   mvn clean package
```
This includes running Apache Kafka via docker containers from
`testcontainers.io`. There is a large, one time, download.
There is a lot of logging output.

This create a jar file `jena-fmod-kafka-VER.jar` in
`jena-fmod-kafka/target/`

Move this jar to 'lib/' in the directory you wish to run Fuseki with the
Fuseki-Kafka connector.

Copy the bash script `fuseki-main` to the same directory.

### Release

Edit and commit `release-setup` to set the correct versions.

```
source release-setup
```

Dry run 
```
mvn $MVN_ARGS -DdryRun=true release:clean release:prepare
```

and for real

```
mvn $MVN_ARGS release:clean release:prepare
mvn $MVN_ARGS release:perform
```

### Run

In the directory where you wish to run Fuseki:

Get a copy of Fuseki Main:

```
wget https://repo1.maven.org/maven2/org/apache/jena/jena-fuseki-server/4.5.0/jena-fuseki-server-4.5.0.jar
```

then run `fuseki-main --conf config.ttl`

where `config.ttl is the configuration file for the server including the
connector setup.

Windows uses can run `fuseki-main.bat` which may need adjusting for the coirrect
version number of Fuseki.

### Publish - deploy with maven

Run
```
   mvn clean deploy
```

### Client

`jena-fuseki-client` contains a script `fk` for operations on the Kafka topic.

`fk send FILE` sends a file, using the file extension for the MIME type.

`fk dump` dumps the Kafka topic.
