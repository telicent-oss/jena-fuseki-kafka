package org.apache.jena.fuseki.kafka;

import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.server.DataAccessPoint;
import org.apache.jena.fuseki.server.DataAccessPointRegistry;
import org.apache.jena.fuseki.server.DataService;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sys.JenaSystem;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
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

    @Test(dataProvider = "paths")
    public void givenEmptyDapRegistry_whenFindingDataset_thenNull(String path) {
        // Given
        DataAccessPointRegistry registry = mock(DataAccessPointRegistry.class);
        when(registry.get(any(String.class))).thenReturn(null);
        FusekiServer server = mock(FusekiServer.class);
        when(server.getDataAccessPointRegistry()).thenReturn(registry);

        // When
        Optional<DatasetGraph> dsg = FKS.findDataset(server, path);

        // Then
        Assert.assertFalse(dsg.isPresent());
    }

    @DataProvider(name = "paths")
    private Object[][] paths() {
        return new Object[][] {
                { "/ds" },
                { "/ds/" },
                { "/ds/upload" },
                { "/ds/upload/"}
        };
    }

    @Test(dataProvider = "paths")
    public void givenNonEmptyDapRegistry_whenFindingDataset_thenFound(String path) {
        // Given
        DatasetGraph dsg = DatasetGraphFactory.empty();
        DataAccessPointRegistry registry = new  DataAccessPointRegistry();
        DataService service = mock(DataService.class);
        when(service.getDataset()).thenReturn(dsg);
        registry.register(new DataAccessPoint("ds", service));
        FusekiServer server = mock(FusekiServer.class);
        when(server.getDataAccessPointRegistry()).thenReturn(registry);

        // When
        Optional<DatasetGraph> found = FKS.findDataset(server, path);

        // Then
        Assert.assertTrue(found.isPresent());
        Assert.assertEquals(found.get(), dsg);
    }
}
