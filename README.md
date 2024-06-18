# jena-fuseki-kafka 

Apache Jena Fuseki extension module for receiving data over
Apache Kafka topics.

License: Apache License 2.0.  
See [LICENSE](./LICENSE).

The Fuseki-Kafka connector receives Kafka events on a topic and applies them to
a Fuseki service endpoint. In effect, it is the same as if an HTTP POST request is sent to
the Fuseki service.

The Kafka event must have a Kafka message header "Content-Type" set to the MIME
type of the content. No other headers are required.

Supported MIME types:
* An RDF triples or quads format - any syntax supported by 
  Apache Jena - `application/n-triples`, `text/turtle`, ...
* SPARQL Update
* [RDF Patch](https://jena.apache.org/documentation/rdf-patch/)

The Fuseki service must be configured with the appropriate operations.
`fuseki:gsp-rw` or `fuseki:upload` for pushing RDF into a dataset; 
`fuseki:update` for SPARQL Update,
`fuseki:patch` for RDF Patch.
The Fuseki implementation of the SPARQL Graph Store Protocol is extended to
support data sent to the dataset without graph name (i.e. quads).

```
:service rdf:type fuseki:Service ;
    fuseki:name "ds" ;
    fuseki:endpoint [ fuseki:operation fuseki:query ] ;
    ...

    # Fuseki-Kafka
    fuseki:endpoint [ fuseki:operation fuseki:update ] ;
    fuseki:endpoint [ fuseki:operation fuseki:gsp-rw ] ;
    fuseki:endpoint [ fuseki:operation fuseki:patch ] ;
    ...
    fuseki:dataset ... ;
    .
```

See "[Configuring Fuseki](https://jena.apache.org/documentation/fuseki2/fuseki-configuration.html)"
for access control and other features.

This project uses the Apache Jena Fuseki Main server and is configured with a
Fuseki configuration file.

Java 17 or later is required.

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
    fk:topic              "env:{ENV_KAFKA_TOPIC:RDF}";
   

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

### Environment variable configuration
As illustrated in the example configuration above with `fk:topic` environment variables (or System Properties) can be 
used when configuring variables. It has two formats, with a default value (if the environment variable is not set) 
or not. 

#### With default - `"env:{ENV_VARIABLE:default}"`
#### Without default - `"env:{ENV_VARIABLE}"`

## Build

Run
```
   mvn clean package
```
This includes running Apache Kafka via docker containers from
`testcontainers.io`. There is a large, one time, download.

This creates a jar file `jena-fmod-kafka-VER.jar` in
`jena-fmod-kafka/target/`

Move this jar to 'lib/' in the directory you wish to run Fuseki with the
Fuseki-Kafka connector.

Copy the bash script `fuseki-main` to the same directory.

### Release

Edit and commit `release-setup` to set the correct versions.

```
source release-setup
```
This prints the dry-run command.

If you need to change the setup, edit this file, commit it and simply source the
file again.

Dry run 
```
mvn $MVN_ARGS -DdryRun=true release:clean release:prepare
```

and for real

```
mvn $MVN_ARGS release:clean release:prepare
```
**NB** Note that a `mvn release:peform` is not required as the release to Maven 
Central will be triggered automatically by the `git tag` created by the release
preparation.

After `release:prepare` the local git repo status may say it is ahead of the
upstream github repo by 2 commits. It isn't - they should be in-step but not 
sync'ed. Do a `git pull` and then `git status` should say "up-to-date". 

### Rollback

If things go wrong, and it is just in the release steps:

```
mvn $MVN_ARGS release:rollback
```

otherwise, checkout out from git or reset the version manually with:

```
mvn versions:set -DnewVersion=...-SNAPSHOT
```

## Run

In the directory where you wish to run Fuseki:

Get a copy of Fuseki Main:

```
wget https://repo1.maven.org/maven2/org/apache/jena/jena-fuseki-server/4.7.0/jena-fuseki-server-4.7.0.jar
```
and place in the current directory.

Get a copy of the script [fuseki-main](https://github.com/Telicent-io/jena-fuseki-kafka/blob/main/fuseki-main)
then run 

```
fuseki-main jena-fuseki-server-4.7.0.jar --conf config.ttl`
```

where `config.ttl is the configuration file for the server including the
connector setup.

Windows uses can run `fuseki-main.bat` which may need adjusting for the correct
version number of Fuseki.

## Client

`jena-fuseki-client` contains a script `fk` for operations on the Kafka topic.

`fk send FILE` sends a file, using the file extension for the MIME type.

`fk dump` dumps the Kafka topic.
