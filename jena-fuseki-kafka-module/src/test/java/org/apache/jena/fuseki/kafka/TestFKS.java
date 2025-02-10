package org.apache.jena.fuseki.kafka;

import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.server.DataAccessPointRegistry;
import org.apache.jena.fuseki.server.DataService;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sys.JenaSystem;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFKS {

    static {
        JenaSystem.init();
        FusekiLogging.markInitialized(true);
    }

    @Test
    public void givenEmptyDapRegistry_whenFindingDataset_thenNull() {
        // Given
        DataAccessPointRegistry registry = mock(DataAccessPointRegistry.class);
        when(registry.get(any(String.class))).thenReturn(null);
        FusekiServer server = mock(FusekiServer.class);
        when(server.getDataAccessPointRegistry()).thenReturn(registry);

        // When
        Optional<DatasetGraph> dsg = FKS.findDataset(server, "/ds");

        // Then
        Assert.assertFalse(dsg.isPresent());
    }
}
