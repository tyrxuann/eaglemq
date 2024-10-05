package io.github.eaglemq.broker.support;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MappedFileAccessorTest {
    private static final String filePath = "src/test/resources/fixtures/broker/store/order_cancel_topic/00000000";

    @BeforeEach
    public void setUp() throws Exception {
        new File(filePath).createNewFile();
    }

    @AfterEach
    public void tearDown() {
        new File(filePath).delete();
    }

    @Test
    void should_write_and_read_from_mapped_file() throws Exception {
        try (MappedFileAccessor accessor = new MappedFileAccessor(filePath, 0, 100 * 1024)) {
            String str = "this is a test content";
            byte[] writeContent = str.getBytes(StandardCharsets.UTF_8);
            accessor.write(writeContent);
            byte[] readContent = accessor.read(0, writeContent.length);
            assertEquals(str, new String(readContent, StandardCharsets.UTF_8));
        }
    }
}
