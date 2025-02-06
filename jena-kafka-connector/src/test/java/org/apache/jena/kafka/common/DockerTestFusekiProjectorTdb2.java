package org.apache.jena.kafka.common;

import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.tdb2.TDB2Factory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;

public class DockerTestFusekiProjectorTdb2 extends AbstractDockerTests {

    @MethodSource("batchSizes")
    @ParameterizedTest
    public void givenFusekiProjector_whenProjectingFromKafkaToTdb2Dataset_thenDataIsLoaded(int batchSize) throws IOException {
        // Given
        Location location = Location.create(Files.createTempDirectory("fuseki-kafka"));
        DatasetGraph dsg = TDB2Factory.connectDataset(location).asDatasetGraph();

        // When and Then
        verifyProjection(batchSize, dsg, "fuseki-kafka-projection-tdb2-");
    }
}
