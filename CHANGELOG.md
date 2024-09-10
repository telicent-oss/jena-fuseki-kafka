# Kafka Connector for Apache Jena Fuseki

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
