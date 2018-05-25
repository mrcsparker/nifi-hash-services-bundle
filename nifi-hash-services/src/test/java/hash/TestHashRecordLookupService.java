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
package hash;

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

public class TestHashRecordLookupService {

    private TestRunner runner;
    private HashRecordLookupService service;

    @Before
    public void init() throws Exception {
        TestProcessor testProcessor = new TestProcessor();
        runner = TestRunners.newTestRunner(testProcessor);

        service = new HashRecordLookupService();
        runner.addControllerService("hash-pii-record-lookup-service", service);
        runner.setProperty(service, HashRecordLookupService.HASH_KEY, "4BAC2739-3BDD-9777-CE02453256C5");
    }

    @Test
    public void testSimpleHash() throws Exception {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("the-key", "sample key");
        final Optional<Record> get1 = service.lookup(criteria);
        assertTrue(get1.isPresent());
        assertEquals("6de3370355ab6ef0c8aa935f9edbe74482242809934bb07f54c712f0d9e562fb", get1.get().getAsString("the-key"));
    }

    @Test
    public void testDoubleHash() throws Exception {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("the-first-key", "sample key");
        criteria.put("the-second-key", "sample key");
        final Optional<Record> get1 = service.lookup(criteria);
        assertTrue(get1.isPresent());
        assertEquals("b62bf8147e9ce794837d39364cff30353ccf819f24e79e56d31f93bc0342a009", get1.get().getAsString("the-first-key"));
        assertEquals("56652afe88563348971138a737e127cf523e29d07544e0a8578af9dd61d3eee2", get1.get().getAsString("the-second-key"));
    }
}
