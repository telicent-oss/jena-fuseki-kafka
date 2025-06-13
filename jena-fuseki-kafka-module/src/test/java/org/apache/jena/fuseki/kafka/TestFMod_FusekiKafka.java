package org.apache.jena.fuseki.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.fuseki.main.FusekiServer;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Set;

public class TestFMod_FusekiKafka {

    private FMod_FusekiKafka createInstance() {
        return new FMod_FusekiKafka();
    }

    @Test
    public void givenModule_whenQueryingName_thenNotBlank() {
        // Given
        FMod_FusekiKafka module = createInstance();

        // When
        String name = module.name();

        // Then
        Assert.assertFalse(StringUtils.isBlank(name));
    }

    @Test
    public void givenModule_whenPreparingWithNoConfigModel_thenNothingToDo() {
        // Given
        FMod_FusekiKafka module = createInstance();

        // When
        module.prepare(Mockito.mock(FusekiServer.Builder.class), Set.of("test"), null);

        // Then
        Assert.assertTrue(module.connectors().isEmpty());
    }
}
