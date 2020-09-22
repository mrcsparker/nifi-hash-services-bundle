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

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestHashRecordLookupService {

    private HashRecordLookupService service;

    @Before
    public void init() throws Exception {
        TestProcessor testProcessor = new TestProcessor();
        TestRunner runner = TestRunners.newTestRunner(testProcessor);

        service = new HashRecordLookupService();
        runner.addControllerService("com.mrcsparker.nifi.hash-pii-record-lookup-service", service);
        runner.setProperty(service, HashRecordLookupService.HASH_KEY, "4BAC2739-3BDD-9777-CE02453256C5");
    }

    @Test
    public void testSimpleHash() throws Exception {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("the-key", "sample key");

        final Optional<Record> get1 = service.lookup(criteria);
        assertTrue(get1.isPresent());
        assertEquals("620a8a34c1f06d4707c12d30c7d20a3b7e9f4d166bbeb28ad5a272883eee0bc5", get1.get().getAsString("the-key"));
    }

    @Test
    public void testDoubleHash() throws Exception {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("the-first-key", "sample key");
        criteria.put("the-second-key", "sample key");

        final Optional<Record> get1 = service.lookup(criteria);
        assertTrue(get1.isPresent());
        assertEquals("620a8a34c1f06d4707c12d30c7d20a3b7e9f4d166bbeb28ad5a272883eee0bc5", get1.get().getAsString("the-first-key"));
        assertEquals("620a8a34c1f06d4707c12d30c7d20a3b7e9f4d166bbeb28ad5a272883eee0bc5", get1.get().getAsString("the-second-key"));
    }

    @Test
    public void testNullHash() throws Exception {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("the-null-key", null);

        final Optional<Record> get1 = service.lookup(criteria);
        assertTrue(get1.isPresent());

        assertNull(get1.get().getAsString("the-null-key"));
    }

    @Test
    public void testEmptylHash() throws Exception {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("the-empty-key", "");

        final Optional<Record> get1 = service.lookup(criteria);
        assertTrue(get1.isPresent());

        assertEquals("", get1.get().getAsString("the-empty-key"));
    }
}
