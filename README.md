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
    # Kafka topic(s)
    # Specify once for each input topic you wish to read
    # Note that each topic MAY only be connected to a single dataset
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

    # Optionally configure a Dead Letter Queue (DLQ) topic
    # 
    # When configured any malformed events are forwarded here
    # If not configured then malformed events cause event processing to abort
    # fk:dlqTopic "env:{ENV_KAFKA_TOPIC:RDF.dlq}" ;

    # Additional Kafka client properties.
    fk:config ( "max.poll.records" "1000" )
    # fk:config ( "key" "value") ;

    # Additional Kafka client properties from an external properties file
    # fk:configFile "/path/to/kafka.properties" ;
    .
```

### Kafka Read Behaviour

Note that the Fuseki Kafka Module by default starts the Kafka connectors prior to the Fuseki HTTP Server starting,
waiting briefly to see if each starts successfully.  The Kafka polling happens on background threads writing into the
targeted dataset.  Therefore Fuseki Kafka is eventually consistent, however in the worst case scenario where there are
active producers writing to the Kafka topic(s) faster than Fuseki can read from them it could never catch up.

As of 2.x Fuseki Kafka uses Kafka Consumer Groups when reading events from the configured Kafka topic(s).  This means
that you **MUST** either have only a single connector in your Fuseki configuration, or each connector **MUST** specify a
`fk:groupId` property with a unique consumer group ID.

Note that if you are intending to use Kafka to sync RDF to multiple Fuseki instances then each Fuseki instance **MUST**
use a unique consumer group ID per topic.  Otherwise your instances may not be assigned any partitions, and thus won't
receive any RDF.

#### Input Topic(s)

The `fk:topic` property on a connector is used to configure one/more topics to which a Kafka connector will subscribe.
You **MUST** specify this property at least once per connector, and **MAY** specify it multiple times if you wish.

The partitioning strategy for input topics depends on the RDF you are intending to apply.  If your incoming RDF events
are only additive, i.e. you only ever add new triples, then you can have as many partitions as you want since the set
semantics of RDF means regardless of the order of event application the dataset will eventually reach the same state.

However, if your workload involves deleting triples via RDF patches then the order of events matters and you **MUST**
have only a single partition otherwise events could be applied out of order and deletes not take affect as intended.

Finally, note that Fuseki Kafka restricts that each input Kafka topic **MAY** only be used by a single connector in your
configuration.

#### `fk:replay` and `fk:sync`

The `fk:replay` and `fk:sync` properties specify boolean values used to configure how Fuseki Kafka reads from the
configured [input topics](#input-topics).  If not specified then `fk:replay` defaults to `false`, and `fk:sync` defaults
to `true`.

If both are set to `true` then `fk:replay` takes precedence, depending on the values of these properties, and the
[`fk:stateFile`](#state-file) property you will get the following read behaviours:

| `fk:replay` | `fk:sync`    | `fk:stateFile` configured? | Read Behaviour                      |
|-------------|--------------|----------------------------|-------------------------------------|
| `true`      | `true/false` | Yes/No                     | From beginning of configured topics |
| `false`     | `true`       | Yes                        | From previously stored offsets in state file, or beginning if no previously stored offsets |
| `false`     | `false`      | Yes/No                     | From most recent offset             |

So depending on your use case you can configure Fuseki Kafka to read the input topic(s) as needed.  For example if you
were running a Fuseki instance with a non-persistent dataset then you'd want to set `fk:replay true` so that the dataset
is rebuilt from the Kafka topic(s) each time Fuseki restarts.

Most users with persistent datsets, e.g. TDB2, will likely want to stick with the default `fk:sync true` behaviour in
combination with configuring a  [`fk:stateFile`](#state-file) so that your Fuseki instance is kept up to date with the
Kafka topic(s) over Fuseki restarts.

#### State File

The `fk:stateFile` property sets the location of the persistent state file on disk, this is used to track Kafka offsets.
If this is not configured then offsets are only tracked on the Kafka brokers and may be ignored on subsequent Fuseki
restarts.

In the 2.x releases the format of this file changed and it is not backwards compatible with 1.x releases.  If you are
upgrading from the 1.x releases then Fuseki Kafka does understand the 1.x format state file and will transform it into
the new format the first time you restart Fuseki with Fuseki Kafka 2.x.x

The state file, and the Kafka Consumer Group offsets, are only updated once Fuseki Kafka has successfully applied and
`commit()`'d events to the target dataset.  Therefore in the event of a service crash event consumption will resume from
the last known committed offset, assuming [default `fk:sync` behaviour](#fkreplay-and-fksync) is in use.

### Batching

In the 1.x releases batching was an opt-in behaviour, from practical experience it has become clear that not using
batching has determinental performance impacts so as of 2.x batching is now core functionality of this module.

Fuseki Kafka batches the application of incoming events from the Kafka topic(s) so that many events may be applied in a
single transaction to reduce the transaction overheads.  This is especially important when using underlying storage like
TDB2, which has copy-on-write semantics, where a transaction per event would quickly exhaust disk space.

The default batch size is `5000` events, this may be controlled by setting the `max.poll.records` Kafka configuration to
the desired value via the `fk:config` property, e.g. in our earlier example this was set to `1000`.  Note that Fuseki
Kafka **does not** guarantee to honour this batch size exactly and uses various heuristics to decide when to commit a
batch, e.g., if it has fully caught up with the Kafka topic (i.e. lag is `0`), if there's events buffered in memory from
earlier Kafka polling, if it hasn't committed in a certain time window etc.  Please see JavaDoc on `FusekiProjector`
which implements the batching logic for more details.  In general it aims to maximise batch size, and minimise the
number of transactions wherever possible, while ensuring timely application of events to the target dataset.

### Error Handling

By default error handling is off as of `2.0.0`, if a malformed event is received on the Kafka topic(s), or an event
cannot be applied, then processing aborts and no further events will be read from Kafka.  This default behaviour was
changed from the 1.x releases as in those it was possible for a malformed/misapplied event to cause an entire batch of
events to be discarded and not applied leading to data loss.

For robust error handling we strongly recommend that you use the new `fk:dlqTopic` property to specify a Dead Letter
Queue (DLQ) topic to which malformed/misapplied events will be written.  A `Dead-Letter-Reason` header will be added to
those events indicating why they were considered malformed, or failed to apply.  When an error occurs the event is
written to the DLQ, and Fuseki Kafka guarantees to apply all prior events in the [batch](#batching) before proceeding.

Note that the DLQ topic **MUST NOT** be a topic that is used as an input topic for a connector as otherwise that would
create an error loop where bad events are injected back into the input topic.

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
