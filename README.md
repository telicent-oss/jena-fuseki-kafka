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

See "[Configuring
Fuseki](https://jena.apache.org/documentation/fuseki2/fuseki-configuration.html)"
for access control and other features.

This project uses the Apache Jena Fuseki Main server and is configured with a
Fuseki configuration file.

## Connector Configuration

The connector configuration goes in the Fuseki confiuration file.

```
PREFIX :        <#>
PREFIX fuseki:  <http://jena.apache.org/fuseki#>
PREFIX fk:      <http://jena.apache.org/fuseki/kafka#>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ja:      <http://jena.hpl.hp.com/2005/11/Assembler#>

<#connector> rdf:type fk:Connector ;
    # Kafka topic
    fk:topic            "RDF";

    # Destination Fuseki service
    # This can be the dataset or a specific endpoint ("ds/kafkaIncoming")
    # with the necessary fuseki:operation.
    
    fk:fusekiServiceName  "ds";

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

# Build

Run
```
   mvn clean package
```
to get a runnable jar in `target/`.
```
   java -jar target/fuseki-kafka-VER.jar --config config.ttl
```
