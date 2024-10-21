# Kafka Connector for Apache Jena Fuseki

## 1.4.0

- Added new `fk:configFile` property to Connector Assembler support to allow injecting an external Kafka configuration 
  file into Fuseki, this allows for advanced configuration e.g. complex Kafka AuthN modes
- Added dependency on Smart Caches Core 0.24.0 (currently for tests only)
- Added integration tests that verify connectivity with Secured Kafka clusters works correctly
- Refactored some tests to use TestNG to make it easier to inject different Kafka clusters into those test via 
  inheritance

## 1.3.6

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
