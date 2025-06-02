# jena-fuseki-kafka 

Apache Jena Fuseki extension module for receiving data over Apache Kafka topics.

The Fuseki-Kafka connector receives Kafka events on a topic and applies them to a Fuseki service endpoint. In effect, it
is the same as if an HTTP POST request is sent to the Fuseki service.

The Kafka event **SHOULD** have a Kafka message header `Content-Type` set to the MIME type of the content, if not
present then NQuads is assumed. No other headers are required.

Supported MIME types:

- An RDF triples or quads format - any syntax supported by Apache Jena - `application/n-triples`, `text/turtle`, ...
- [RDF Patch](https://jena.apache.org/documentation/rdf-patch/)

See [Configuring Fuseki](https://jena.apache.org/documentation/fuseki2/fuseki-configuration.html) for general Fuseki
configuration documentation.  This project uses the Apache Jena Fuseki Main server and is configured with a Fuseki
configuration file.

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
   
    # Destination Fuseki service. This is the base URI path for the dataset
    
    fk:fusekiServiceName  "/ds";

    # Specify the Kafka bootstrap servers
    fk:bootstrapServers   "localhost:9092";

    # File used to track the state (the last offset processes)
    # Used across Fuseki restarts.
    fk:stateFile        "Databases/RDF.state";

    # Kafka Consumer GroupId - default "JenaFusekiKafka"
    # MUST be unique across all connectors in your config file
    # If you are running multiple Fuseki instances then this MUST be unique for each instance otherwise only a single
    # instance will actually read data from the Kafka topic
    # fk:groupId          "JenaFusekiKafka";

    # What to do on start up.
    # Normally this is "sync" which is the default value.
    # If replay is true, then on start-up the whole topic is replayed.
    #    fk:replayTopic      true;
    #    fk:syncTopic        true;

   ## Additional Kafka client properties.
   ## fk:config ( "key" "value") ;

   ## Additional Kafka client properties from an external properties file
   ## fk:configFile "/path/to/kafka.properties" ;
    .
```

Note that the Fuseki Kafka Module by default starts the Kafka connectors prior to the Fuseki HTTP Server starting,
waiting briefly to see if each starts successfully.  The Kafka polling happens on background threads writing into the
targeted dataset.  If there are active producers writing to the Kafka topic(s) faster than Fuseki can read from them it
could never catch up, and never start servicing HTTP requests.

### Environment variable configuration

As illustrated in the example configuration above with `fk:topic` environment variables (or System Properties) can be 
used when configuring variables. It has two formats, with a default value (if the environment variable is not set) 
or not. 

#### With default - `"env:{ENV_VARIABLE:default}"`

Note that it is possible for the default to be the empty string e.g. `env:{ENV_VARIABLE:}`, **however** most
configuration is required so supplying an empty value is generally not permitted.

#### Without default - `"env:{ENV_VARIABLE}"`

When the without default form is used, if the relevant environment variable is not set then an error is thrown which
will typically prevent the connector from being created, and the server into which the module is embedded running.

### Additional Kafka Configuration

If extra Kafka configuration properties are required to connect to your Kafka cluster, e.g. you require Kafka
Authentication, then you can supply this in one of two ways:

1. Inline in the RDF configuration via the `fk:config` property
2. Externally via a properties file referenced from the RDF configuration via the `fk:configFile` property

#### Inline Kafka Configuration

For each Kafka property you wish to set you need to declare a triple which has a RDF collection as its object, this
collection **MUST** have two items in it where the first is considered the property key and the second the property
value e.g.

```
<#connector> fk:config ( "security.protocol" "SSL") .
```

Would set the Kafka `security.protocol` property to the value `SSL` in the configuration properties passed to the
underlying Kafka consumer.

Multiple properties require multiple triples e.g.

```
<#connector> fk:config ( "security.protocol" "SSL") ,
                       ( "ssl.truststore.location" "/path/to/truststore" ).
```

#### External Kafka Configuration

For some clusters setups, e.g. Kafka mTLS authentication, then it may be necessary to supply many configuration
properties, some of which contain sensitive values that you don't want visible in your RDF configuration.  In this case
you can create a separate Java properties file that defines the additional Kafka properties you need and pass this via
the `fk:configFile` property e.g.

```
<#connector> fk:configFile "/path/to/kafka.properties" .
```
The object of this triple may be one of the following:

- A string literal with a file path
- A `file:` URI e.g. `<file:/path/to/kafka.properties>`
- A string literal using [Environment Variables](#environment-variable-configuration) e.g.
  `"env:{KAFKA_CONFIG_FILE_PATH:}"` .
- A `env:` URI e.g. `<env:{KAFKA_CONFIG_FILE_PATH:}>`

Note that if this triple is present but gives an empty value then it is ignored, this can be useful if you want to
define a RDF configuration where you can conditionally inject a Kafka properties file via an environment variable where
necessary.

##### Multiple Kafka Properties Files

If multiple `fk:configFile` triples are present then all the referenced properties files are loaded, however the order
of loading is not defined.  Therefore we recommend that you only split your Kafka configuration over multiple properties
files if they are not going to override each others configuration.

#### Configuring for Kafka Authentication

Using the mechanisms outlined above it is possible to configure the connectors to talk to secure Kafka clusters, the
exact set of Kafka configuration properties required will depend upon your Kafka cluster and the authentication mode
being used.  Therefore we would refer you to the [Kafka Security](https://kafka.apache.org/documentation/#security)
documentation for full details.

If you are using a managed Kafka cluster, e.g. Confluent Cloud, Amazon MKS etc., you may also find your vendor has
extensive documentation on how to enable and use Kafka security with their offerings, this will again mostly come down
to supplying the appropriate Kafka configuration properties using the mechanisms we have already outlined.

You might also find our own [Kafka Connectivity
Options](https://github.com/telicent-oss/smart-caches-core/blob/main/docs/cli/index.md#kafka-connectivity-options)
documentation useful.  Note that while the options and environment variables discussed there **DO NOT** apply to this
repository since that is for CLIs and this is a library, the examples of Kafka properties for different Kafka
Authentication modes are applicable.

## Build

Run

```
mvn clean package
```
This includes running Apache Kafka via docker containers from `testcontainers.io`. There is a large, one time, download.

This creates a jar file `jena-fmod-kafka-VER.jar` in `jena-fmod-kafka/target/`

Move this jar to 'lib/' in the directory you wish to run Fuseki with the Fuseki-Kafka connector.

Copy the bash script `fuseki-main` to the same directory.

### Release

Edit and commit `release-setup` to set the correct versions.

```
source release-setup
```
This prints the dry-run command.

If you need to change the setup, edit this file, commit it and simply source the file again.

Dry run 
```
mvn $MVN_ARGS -DdryRun=true release:clean release:prepare
```
and for real

```
mvn $MVN_ARGS release:clean release:prepare
```
**NB** Note that a `mvn release:peform` is not required as the release to Maven Central will be triggered automatically
by the `git tag` created by the release preparation.

After `release:prepare` the local git repo status may say it is ahead of the upstream github repo by 2 commits. It isn't
- they should be in-step but not sync'ed. Do a `git pull` and then `git status` should say "up-to-date". 

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

Get a copy of Fuseki Main from the [Apache Jena
Downloads](https://jena.apache.org/download/index.cgi#apache-jena-binary-distributions) page and place in the current
directory.

Use the script [`fuseki-main`](https://github.com/Telicent-io/jena-fuseki-kafka/blob/main/fuseki-main) from this
repository then run:

```
fuseki-main jena-fuseki-server-4.7.0.jar --conf config.ttl
```

Where `config.ttl` is the configuration file for the server including the connector setup.  Windows uses can run
`fuseki-main.bat` which may need adjusting for the correct version number of Fuseki.

# License

This code is Copyright Telicent Ltd and licensed under Apache License 2.0.  See [LICENSE](./LICENSE).
