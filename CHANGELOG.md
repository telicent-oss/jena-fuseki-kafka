# Kafka Connector for Apache Jena Fuseki

## 2.1.0

- `FusekiProjector` batching improvements:
    - The automatic batching logic in `FusekiProjector` has been improved to handle two key edge cases observed in
      production usage:
        - When experiencing high lag then batching by number of events is disabled and batching is instead done by
          amount of data received.  This works particularly well when far behind a topic containing lots of small
          messages as it reduces the number of transactions which speeds up processing.
        - When connecting to a low volume topic, or the input topic is populated by a slow producer, such that we are
          frequently up to date (lag=0) and committing small batches then committing at zero lag is disabled and instead
          done only when batch size/max transaction duration is reached.  This reduces the number of transactions for
          such topics again improving performance by limiting transaction overheads.
        - Both of these modes automatically switch back to normal batching behaviour as and when the triggering
          conditions are no longer present.
    - Added support for custom Fuseki Kafka specific properties to be supplied to control/disable all of the above
      behaviours
- Build and Test improvements:
    - Added dependency on Apache Commons Math 3.6.1
    - Lombok upgraded to 1.18.40
    - Smart Caches Core upgraded to 0.29.5
    - Various build and test dependencies upgraded to latest available

## 2.0.2

- Fixed a bug where upgrading legacy state files caused Kafka offsets to be off by 1 resulting in re-processing an event
  from the input topic
- Picked up a fix for Kafka polling where long transaction lock waits (caused by interactions with external code) could
  lead to Kafka polling threads failing prematurely.  These threads will now auto-recover in this scenario.
- Build and Dependency Updates:
    - Switched to new Maven Central publishing process
    - Apache Commons IO upgraded to 2.20.0
    - Apache Commons Lang upgraded to 3.18.0
    - Apache Jena upgraded to 5.5.0
    - Log4j upgraded to 2.25.1
    - Smart Caches Core upgraded to 0.29.2
    - Various build and test dependencies upgraded to latest available

## 2.0.1

- Fixed a bug where old 1.x configurations that used full endpoint URIs, e.g. `/ds/upload`, for their connector
  `fk:fusekiServiceName` properties, as opposed to simple service names, i.e. `/ds`, would not successfully start. These
  service names should now successfully permit startup with a warning that support for full endpoint URIs in
  configuration will be removed in the future.
- Fixed a couple of cases where a NPE could be thrown during startup without an accompanying descriptive error

## 2.0.0

This is a major version with significant breaking changes versus the 1.x releases.  These changes have been motivated by
reducing Telicent's code maintenance burden by adopting common libraries we use across our products that standardise how
we interact with Kafka, especially as those Core Libraries are much more heavily tested in a wide variety of unit and
integration tests than the existing one-off code in this repository was.

### Breaking Changes

- State file format updated, legacy state files will be automatically read and converted (with warnings) to the new
  format the first time you use it
    - `PersistentState` and `DataState` classes that handled state replaced with new `FusekiOffsetStore` that uses Smart
      Cache Core library APIs
- Removed `FKProcessor`, `FKBatchProcessor` and related implementation classes
    - Replaced with `FusekiSink` and `FusekiProjector` to use Smart Cache Core library APIs
- Removed support for receiving SPARQL Updates over Kafka
    - This was brittle because the update may have different effects depending on the state of the Fuseki instance
      receiving it at the time of reception
    - See `SPARQL_Update_CQRS` in the https://github.com/telicent-oss/smart-cache-graph repository for a way to turn
      SPARQL Updates into RDF Patches that Fuseki Kafka can apply
- Removed start up checks of Kafka connectivity
    - The Smart Cache Core libraries we are using include various connectivity, and topic existence checking, plus error
      handling and logging for lots more Kafka failure scenarios.  These scenarios will now be surfaced in the Fuseki
      logs so should be monitored for and addressed accordingly.
- The module no longer requires that connectors be fully up to date with Kafka topics prior to allowing Fuseki to start
  and service requests.
    - This fixes a bug where Fuseki Kafka could find itself in a crash restart loop if external monitoring health checks
      were trying to make HTTP requests to see if it was ready to service requests.  This crash restart loop would
      continue until such time as it caught up with the topic(s), assuming it did catch up.
- Removed unimplemented support for dispatching events to a remote endpoint
    - **NB** This was a placeholder for future functionality for which we had no actual use cases
- Removed deprecated `MockKafka`
    - We were already using `KafkaTestCluster` from Smart Cache Core libraries for all integration tests anyway
- Removed `jena-kafka-client` module
    - `debug` CLI from Smart Caches Core provides more general equivalent functionality -
      https://github.com/telicent-oss/smart-caches-core/blob/main/docs/cli/debug.md

### New Features

- Introduced Telicent Smart Cache Core libraries to handle all the Kafka read/write logic.  These APIs provide wrappers
  around the lower level Kafka APIs that make it easier for us to handle errors, avoid lost events etc.
- Kafka Connectors can now be optionally configured with a Dead Letter Queue (DLQ) to which bad events are forwarded
    - If no DLQ is configured the Kafka Connector will abort
- Kafka Connectors can now be configured with multiple topics so a single connector can read events from multiple topics
  into a single dataset
    - Previously this required configuring a connector for each unique topic
- Kafka Connectors now support reading events from multi-partition topics
    - Previously they would only read from partition `0` regardless of how many partitions the topic had
- Batching behaviour is now handled automatically and aims to maximise transaction batch size based on available events

### Bug Fixes

- Fixed a bug where stopping a server would not guarantee to stop and de-register existing Kafka connectors, this is
  especially relevant for uses who embed Fuseki in another process
- Fixed a bug where a bad event would cause an entire batch of Kafka messages to not be processed potentially losing
  some data
    - Provided that a DLQ is configured the module now guarantees that all events prior to the bad event are properly
      applied so no data is lost
- Fixed a bug where Consumer Group was ignored and never actually used
    - Previously this could be configured but due to how the Kafka APIs were used it was effectively ignored
- Fixed a bug where Consumer Group was suffixed with a random UUID
    - This meant that every time you ran SCG you got a unique Consumer Group, due to the above bug this actually had no
      effect but now that bug is fixed we also had to fix this otherwise every time it restarted it would potentially
      create a new Consumer Group
    - **NB** This does mean that if you are running multiple instances of Fuseki with Kafka Connectors each instance
      **MUST** now have a unique Consumer Group otherwise only one instance will be assigned the topic partitions for
      the configured topic(s) so only one instance will be kept up to date.
    - If you want to have a single configuration file consider using the environment variable interpolation feature to
      inject a per-instance unique value into the consumer group, e.g. `fk:groupId "env:POD_NAME"`
- Fixed a bug where multiple connectors with the same Consumer Group would get the application into a group rebalance
  loop that resulted in it never being assigned partitions
    - Each connector in the configuration **MUST** now be assigned a unique Consumer Group
- Fixed a bug where otherwise well-formed RDF patches could fail to apply due to interactions with the external
  transaction that the event processing was wrapping around them
- Fixed a bug where malformed configuration files could lead to either a NPE, or silently ignoring a malformed Kafka
  connector during startup
    - These should now throw an error that halts Fuseki startup

### Build and Test Improvements

- Upgraded Apache Commons BeanUtils to 1.11.0
- Upgraded Apache Jena to 5.4.0
- Upgraded Apache Kafka to 3.9.1
- Upgraded Smart Caches Core Libraries to 0.29.1
- Upgraded various build and test dependencies to latest available

## 1.5.3

- Upgraded Apache Jena to 5.3.0
- Upgraded various build and test dependencies to latest available

## 1.5.2

- Adding backup/restore functionality.

## 1.5.1

- Added support for `<env:VAR>` URIs for `fk:configFile` property
- Allowed for `<env:{VAR:}>`, i.e. blank default, to be used so that a config file can be created that loads in extra
  Kafka configuration only when that environment variable is set

## 1.5.0

- Added new `fk:configFile` property to Connector Assembler support to allow injecting an external Kafka configuration 
  file into Fuseki, this allows for advanced configuration e.g. complex Kafka AuthN modes
- Added dependency on Smart Caches Core 0.24.0 (currently for tests only)
- Added integration tests that verify connectivity with Secured Kafka clusters works correctly
- Refactored some tests to use TestNG to make it easier to inject different Kafka clusters into those test via 
  inheritance

## 1.4.0

- `FMod_FusekiKafka` makes `startKafkaConnectors()` a protected method to allow derived modules flexibility in deciding
  when to start the Kafka Connectors as there are trade off involved, see Javadoc on that method for discussion
- Upgraded Protobuf to 4.27.5
- Various build and test dependencies upgraded to latest available

## 1.3.5

- Improved Kafka batching strategy to further reduce small batch consumption of Kafka records where possible
- Apache Log4j upgraded to 2.24.0
- Various build and test dependencies upgraded to latest available

## 1.3.4

- Changed Kafka polling durations to be consistenly longer to avoid processing too small batches when the consumer is
  caught up with the producer
- Upgraded various test and build dependencies to latest available

## 1.3.3

- Apache Jena upgraded to 5.1.0
- RDF-ABAC upgraded to 0.71.3
- Upgraded various test and build dependencies to latest available

## 1.3.2

- First release to Maven Central
