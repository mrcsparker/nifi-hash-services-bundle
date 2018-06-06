package com.mrcsparker.nifi.hash;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestKeyHashRecord {
    private TestRunner runner;
    private MockRecordParser readerService;
    private MockRecordWriter writerService;

    @Before
    public void setup() throws InitializationException {
        readerService = new MockRecordParser();
        writerService = new MockRecordWriter("header", false);

        runner = TestRunners.newTestRunner(KeyHashRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(HashRecord.RECORD_READER, "reader");
        runner.setProperty(HashRecord.RECORD_WRITER, "writer");
        runner.setProperty(HashRecord.HASH_KEY, "4BAC2739-3BDD-9777-CE02453256C5");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("address", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);
        readerService.addSchemaField("state", RecordFieldType.STRING);
        readerService.addSchemaField("city", RecordFieldType.STRING);
    }

    @Test
    public void testSimpleHash() {
        runner.setProperty("a", "/name");
        runner.setProperty("b", "/address");
        runner.enqueue("");
        runner.setValidateExpressionUsage(false);

        readerService.addRecord("sample key", "123 address", 35, "", "");
        runner.run();

        runner.assertAllFlowFilesTransferred(HashRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(HashRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("header\ne0017d72714affa14ee435f57450d4364d2161e2f0abf90019964f1263c1d073,sample key\nebb59f4d65174fa86f81d0b37f2ee2d3d6dff4f1ccebc303416cf78e488a083e,123 address\n");
    }
}
