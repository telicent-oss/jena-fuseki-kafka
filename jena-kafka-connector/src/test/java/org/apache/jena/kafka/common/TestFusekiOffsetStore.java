package org.apache.jena.kafka.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.telicent.smart.cache.sources.kafka.KafkaEventSource;
import org.apache.jena.kafka.JenaKafkaException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TestFusekiOffsetStore {

    private static final String DATASET_NAME = "/ds";
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void givenNullDatasetName_whenCreatingStore_thenIllegalArgument() {
        // Given, When and Then
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> FusekiOffsetStore.builder().datasetName(null).build());
    }

    @Test
    public void givenValidDatasetName_whenCreatingStore_thenOK_andSaveIsNoOp() {
        // Given
        String datasetName = DATASET_NAME;

        // When
        FusekiOffsetStore store = FusekiOffsetStore.builder().datasetName(datasetName).build();

        // Then
        Assertions.assertNotNull(store);
        Assertions.assertEquals(datasetName, store.getDatasetName());

        // And
        store.flush();
    }

    @Test
    public void givenLegacyStateFile_whenCreatingStore_thenConvertedOk() throws IOException {
        // Given
        String datasetName = DATASET_NAME;
        Map<String, Object> legacyState =
                Map.of(FusekiOffsetStore.FIELD_DATASET, datasetName, FusekiOffsetStore.LEGACY_FIELD_ENDPOINT, "foo",
                       FusekiOffsetStore.LEGACY_FIELD_TOPIC, "test", FusekiOffsetStore.LEGACY_FIELD_OFFSET, 1234L);
        File stateFile = createStateFile(legacyState);

        // When
        FusekiOffsetStore store = FusekiOffsetStore.builder()
                                                   .datasetName(datasetName)
                                                   .stateFile(stateFile)
                                                   .consumerGroup("example")
                                                   .build();

        // Then
        Assertions.assertEquals(datasetName, store.getDatasetName());
        Assertions.assertTrue(store.hasOffset(KafkaEventSource.externalOffsetStoreKey("test", 0, "example")));
    }

    private File createStateFile(Map<String, Object> state) throws IOException {
        File stateFile = Files.createTempFile("state", ".json").toFile();
        this.mapper.writeValue(stateFile, state);
        return stateFile;
    }

    @Test
    public void givenNonExistentStateFile_whenCreatingStore_thenNothingIsRead_andOnCloseStateFileIsCreated() {
        // Given
        File stateFile = new File("no-such-state.json");
        stateFile.delete();
        try {
            Assertions.assertFalse(stateFile.exists());

            // When
            FusekiOffsetStore store =
                    FusekiOffsetStore.builder().datasetName(DATASET_NAME).stateFile(stateFile).build();

            // Then
            Assertions.assertTrue(store.offsets().isEmpty());

            // And
            store.close();
            Assertions.assertTrue(stateFile.exists());
            Assertions.assertNotEquals(0, stateFile.length());
        } finally {
            stateFile.delete();
        }
    }

    @Test
    public void givenCurrentStateFile_whenCreatingStore_thenStateIsRead() throws IOException {
        // Given
        Map<String, Object> state =
                Map.of(FusekiOffsetStore.FIELD_DATASET, DATASET_NAME, FusekiOffsetStore.FIELD_OFFSETS,
                       Map.of("test-0-consumer", 7L, "test-1-consumer", 3L));
        File stateFile = createStateFile(state);

        // When
        FusekiOffsetStore store = FusekiOffsetStore.builder().datasetName(DATASET_NAME).stateFile(stateFile).build();

        // Then
        Assertions.assertFalse(store.offsets().isEmpty());
        Assertions.assertEquals(7L, (Long) store.loadOffset("test-0-consumer"));
        Assertions.assertEquals(3L, (Long) store.loadOffset("test-1-consumer"));
    }

    private static Stream<Arguments> malformedStatesProvider() {
        //@formatter:off
        return Stream.of(Arguments.of(Map.of(FusekiOffsetStore.FIELD_DATASET, "")),
                         Arguments.of(Map.of(FusekiOffsetStore.FIELD_DATASET, DATASET_NAME,
                                             FusekiOffsetStore.LEGACY_FIELD_OFFSET, "foo")),
                         Arguments.of(Map.of(FusekiOffsetStore.FIELD_DATASET, DATASET_NAME,
                                             FusekiOffsetStore.FIELD_OFFSETS, List.of(1, 2, 3))),
                         Arguments.of(Map.of(FusekiOffsetStore.FIELD_DATASET, "/other")),
                         Arguments.of(Map.of(FusekiOffsetStore.FIELD_DATASET, DATASET_NAME,
                                             FusekiOffsetStore.LEGACY_FIELD_TOPIC, "test",
                                             FusekiOffsetStore.LEGACY_FIELD_OFFSET, 178L)));
        //@formatter:on
    }

    @ParameterizedTest
    @MethodSource("malformedStatesProvider")
    public void givenMalformedStateFile_whenCreatingStore_thenError(Map<String, Object> malformedState) throws
            IOException {
        // Given
        File stateFile = createStateFile(malformedState);

        // When and Then
        Assertions.assertThrows(JenaKafkaException.class, () -> FusekiOffsetStore.builder()
                                                                                 .datasetName(DATASET_NAME)
                                                                                 .stateFile(stateFile)
                                                                                 .build());

    }

    @Test
    public void givenNonReadableFile_whenCreatingStore_thenError() throws IOException {
        // Given
        File stateFile = this.createStateFile(Map.of(FusekiOffsetStore.FIELD_DATASET, DATASET_NAME));
        stateFile.setReadable(false);

        // When and Then
        Assertions.assertThrows(JenaKafkaException.class, () -> FusekiOffsetStore.builder()
                                                                                 .datasetName(DATASET_NAME)
                                                                                 .stateFile(stateFile)
                                                                                 .build());
    }

    @Test
    public void givenNonWritableFile_whenSavingStore_thenError() throws IOException {
        // Given
        File stateFile = Files.createTempFile("state", ".json").toFile();
        stateFile.setWritable(false);

        // When
        FusekiOffsetStore store = FusekiOffsetStore.builder().datasetName(DATASET_NAME).stateFile(stateFile).build();
        Assertions.assertThrows(JenaKafkaException.class, () -> store.flush());
    }
}
