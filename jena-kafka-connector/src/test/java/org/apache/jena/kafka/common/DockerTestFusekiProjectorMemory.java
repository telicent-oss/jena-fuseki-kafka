package org.apache.jena.kafka.common;

import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class DockerTestFusekiProjectorMemory extends AbstractDockerTests {

    @MethodSource("batchSizes")
    @ParameterizedTest
    public void givenFusekiProjector_whenProjectingFromKafkaToInMemoryTransactionalDataset_thenDataIsLoaded(
            int batchSize) {
        // Given
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();

        // When and Then
        verifyProjection(batchSize, dsg, "fuseki-kafka-projection-mem-transactional-");
    }
}
