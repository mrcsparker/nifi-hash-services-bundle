/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

public class TestHashRecord {
    private TestRunner runner;
    private MockRecordParser readerService;

    @Before
    public void setup() throws InitializationException {
        readerService = new MockRecordParser();
        MockRecordWriter writerService = new MockRecordWriter("header", false);

        runner = TestRunners.newTestRunner(HashRecord.class);
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
    }

    @Test
    public void testSimpleHash() {
        runner.setProperty("/name", "/name");
        runner.enqueue("");
        runner.setValidateExpressionUsage(false);

        readerService.addRecord("sample key", "", 35);
        runner.run();

        runner.assertAllFlowFilesTransferred(HashRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(HashRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("header\ne0017d72714affa14ee435f57450d4364d2161e2f0abf90019964f1263c1d073,,35\n");
    }

    @Test
    public void testMultiHash() {
        runner.setProperty("/name", "/name");
        runner.setProperty("/address", "/address");
        runner.enqueue("");
        runner.setValidateExpressionUsage(false);

        readerService.addRecord("sample key", "123 Foo Way", 35);
        runner.run();

        runner.assertAllFlowFilesTransferred(HashRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(HashRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("header\ne0017d72714affa14ee435f57450d4364d2161e2f0abf90019964f1263c1d073,3763dc3aa103d838243f7a585a498c380fd373577538b3bd7c00b2a1aa72e57a,35\n");
    }

    @Test
    public void testSha512() {
        runner.setProperty("/name", "/name");
        runner.setProperty("/address", "/address");
        runner.setProperty(HashUtils.HASH_ALGORITHM, HashUtils.HASH_SHA512);
        runner.enqueue("");
        runner.setValidateExpressionUsage(false);

        readerService.addRecord("sample key", "123 Foo Way", 35);
        runner.run();

        runner.assertAllFlowFilesTransferred(HashRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(HashRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("header\ncd9d9b85532f02fb977a2742d4c0b388aa317007feb8809e6e58059424a0048eaf0e062e32d12510a006409f58ee664d119d12ee0d75e71d1a84b18c574104ef,6795911664a56c00c0b4a416d0ef199df40ab4b83160246df48fcf00826a97ab1a453fe5b0448795d64739c61771caefb9de89c2607cbb0b21887e2279fe5b3b,35\n");
    }

    @Test
    public void testValuesWithoutPaths() {
        runner.setProperty("/name", "/name");
        runner.setProperty("/address", "/address");
        runner.setProperty("/dog", "/dog");
        runner.setProperty("/car", "/cat");
        runner.setProperty("/yes", "/okay");
        runner.enqueue("");
        runner.setValidateExpressionUsage(false);

        readerService.addRecord("sample key", "123 Foo Way", 35);
        runner.run();

        runner.assertAllFlowFilesTransferred(HashRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(HashRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("header\ne0017d72714affa14ee435f57450d4364d2161e2f0abf90019964f1263c1d073,3763dc3aa103d838243f7a585a498c380fd373577538b3bd7c00b2a1aa72e57a,35\n");
    }
}
