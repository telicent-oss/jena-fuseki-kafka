package org.apache.jena.kafka.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.telicent.smart.cache.sources.kafka.KafkaEventSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.kafka.JenaKafkaException;
import org.apache.jena.sys.JenaSystem;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TestFusekiOffsetStore {
    static {
        JenaSystem.init();
    }

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
        String key = KafkaEventSource.externalOffsetStoreKey("test", 0, "example");
        Assertions.assertTrue(store.hasOffset(key));
        Assertions.assertEquals(1235L, (Long) store.loadOffset(key),
                                "Legacy state file offsets were off by 1 so upgrading should correct that");
    }

    @Test
    public void givenLegacyStateFileWithMisMatchedDatasetName_whenCreatingStore_thenConversionFails() throws
            IOException {
        // Given
        String datasetName = DATASET_NAME;
        Map<String, Object> legacyState = Map.of(FusekiOffsetStore.FIELD_DATASET, datasetName + "/upload",
                                                 FusekiOffsetStore.LEGACY_FIELD_ENDPOINT, "foo",
                                                 FusekiOffsetStore.LEGACY_FIELD_TOPIC, "test",
                                                 FusekiOffsetStore.LEGACY_FIELD_OFFSET, 1234L);
        File stateFile = createStateFile(legacyState);

        // When and Then
        JenaKafkaException e = Assertions.assertThrows(JenaKafkaException.class, () -> FusekiOffsetStore.builder()
                                                                                                        .datasetName(
                                                                                                                datasetName)
                                                                                                        .stateFile(
                                                                                                                stateFile)
                                                                                                        .consumerGroup(
                                                                                                                "example")
                                                                                                        .build());
        Assertions.assertTrue(StringUtils.contains(e.getMessage(), "Dataset name does not match"));
    }

    private File createStateFile(Map<String, Object> state) throws IOException {
        File stateFile = Files.createTempFile("state", ".json").toFile();
        writeStateFile(state, stateFile);
        return stateFile;
    }

    private void writeStateFile(Map<String, Object> state, File stateFile) throws IOException {
        this.mapper.writeValue(stateFile, state);
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

    @Test
    public void givenNonWritableDirectory_whenSavingStore_thenError() throws IOException {
        // Given
        File tempDir = Files.createTempDirectory("test").toFile();
        tempDir.setWritable(false);
        File stateFile = new File(tempDir, "state.json");

        // When
        FusekiOffsetStore store = FusekiOffsetStore.builder().datasetName(DATASET_NAME).stateFile(stateFile).build();
        Assertions.assertThrows(JenaKafkaException.class, () -> store.flush());
    }

    @Test
    public void givenOffsetStore_whenCopyingToSameFile_thenIllegalArgument() throws IOException {
        // Given
        File stateFile = Files.createTempFile("state", ".json").toFile();
        FusekiOffsetStore store = FusekiOffsetStore.builder().datasetName(DATASET_NAME).stateFile(stateFile).build();

        // When and Then
        Assertions.assertThrows(IllegalArgumentException.class, () -> store.copyTo(stateFile));
    }

    @Test
    public void givenOffsetStore_whenCopyingToDifferentFile_thenSuccess_andOffsetsAreSame() throws IOException {
        // Given
        File stateFile = Files.createTempFile("state", ".json").toFile();
        FusekiOffsetStore store = FusekiOffsetStore.builder().datasetName(DATASET_NAME).stateFile(stateFile).build();
        store.saveOffset("test", 1234L);

        // When
        File copy = Files.createTempFile("copy", ".json").toFile();
        Assertions.assertEquals(0, copy.length());
        store.copyTo(copy);

        // Then
        Assertions.assertNotEquals(0, copy.length());
        FusekiOffsetStore copied = FusekiOffsetStore.builder().datasetName(DATASET_NAME).stateFile(copy).build();
        Assertions.assertEquals(1234L, (Long) copied.loadOffset("test"));
    }

    private static @NotNull File createLargeFile(int targetSize) throws IOException {
        File stateFile = Files.createTempFile("state", ".json").toFile();
        try (FileWriter fw = new FileWriter(stateFile)) {
            fw.write("{");
            String oneKb = StringUtils.repeat(" ", 1024);
            for (int i = 0; i < targetSize; i += 1024) {
                fw.write(oneKb);
            }
        }
        return stateFile;
    }

    @Test
    public void givenTooLargeStateFile_whenReadingStore_thenDiscarded() throws IOException {
        // Given
        File stateFile = createLargeFile(10 * 1024 * 1024);

        // When
        FusekiOffsetStore store = FusekiOffsetStore.builder().datasetName(DATASET_NAME).stateFile(stateFile).build();

        // Then
        File discarded = new File(stateFile.getAbsolutePath() + FusekiOffsetStore.DISCARDED_EXTENSION);
        Assertions.assertFalse(stateFile.exists());
        Assertions.assertTrue(discarded.exists());
        Assertions.assertFalse(store.hasOffset("test"));
    }

    @Test
    public void givenTooLargeStateFileWithBackupAvailable_whenReadingStore_thenBackupRestored() throws IOException {
        // Given
        File stateFile = createLargeFile(10 * 1024 * 1024);
        File backupStateFile = new File(stateFile.getAbsolutePath() + FusekiOffsetStore.BACKUP_EXTENSION);
        Map<String, Object> backupState =
                Map.of(FusekiOffsetStore.FIELD_DATASET, "/ds", FusekiOffsetStore.FIELD_OFFSETS, Map.of("test", 345L));
        writeStateFile(backupState, backupStateFile);
        Assertions.assertTrue(backupStateFile.exists());

        // When
        FusekiOffsetStore store = FusekiOffsetStore.builder().datasetName(DATASET_NAME).stateFile(stateFile).build();

        // Then
        File discarded = new File(stateFile.getAbsolutePath() + FusekiOffsetStore.DISCARDED_EXTENSION);
        Assertions.assertTrue(stateFile.exists());
        Assertions.assertTrue(discarded.exists());
        Assertions.assertEquals(345L, (Long) store.loadOffset("test"));
    }

    @Test
    public void givenTooLargeStateFileWithTempAvailable_whenReadingStore_thenTempRestored() throws IOException {
        // Given
        File stateFile = createLargeFile(10 * 1024 * 1024);
        File tempStateFile = new File(stateFile.getAbsolutePath() + FusekiOffsetStore.TEMP_EXTENSION);
        Map<String, Object> backupState =
                Map.of(FusekiOffsetStore.FIELD_DATASET, "/ds", FusekiOffsetStore.FIELD_OFFSETS, Map.of("test", 345L));
        writeStateFile(backupState, tempStateFile);
        Assertions.assertTrue(tempStateFile.exists());

        // When
        FusekiOffsetStore store = FusekiOffsetStore.builder().datasetName(DATASET_NAME).stateFile(stateFile).build();

        // Then
        verifyDiscardFileExists(stateFile, FusekiOffsetStore.DISCARDED_EXTENSION);
        Assertions.assertTrue(stateFile.exists());
        Assertions.assertEquals(345L, (Long) store.loadOffset("test"));
    }

    private static void verifyDiscardFileExists(File stateFile, String expectedExtension) {
        File discarded = new File(stateFile.getAbsolutePath() + expectedExtension);
        Assertions.assertTrue(discarded.exists());
    }

    @Test
    public void givenTooLargeStateFileWithTooLargeBackupAndTempFiles_whenReadingStore_thenDiscarded() throws
            IOException {
        // Given
        File stateFile = createLargeFile(10 * 1024 * 1024);
        Files.copy(stateFile.toPath(),
                   new File(stateFile.getAbsolutePath() + FusekiOffsetStore.BACKUP_EXTENSION).toPath(),
                   StandardCopyOption.REPLACE_EXISTING);
        Files.copy(stateFile.toPath(),
                   new File(stateFile.getAbsolutePath() + FusekiOffsetStore.TEMP_EXTENSION).toPath(),
                   StandardCopyOption.REPLACE_EXISTING);

        // When
        FusekiOffsetStore store = FusekiOffsetStore.builder().datasetName(DATASET_NAME).stateFile(stateFile).build();

        // Then
        verifyDiscardFileExists(stateFile, FusekiOffsetStore.DISCARDED_EXTENSION);
        verifyDiscardFileExists(stateFile, FusekiOffsetStore.DISCARDED_EXTENSION + "-1");
        verifyDiscardFileExists(stateFile, FusekiOffsetStore.DISCARDED_EXTENSION + "-2");
        Assertions.assertFalse(stateFile.exists());
        Assertions.assertFalse(store.hasOffset("test"));
    }
}
