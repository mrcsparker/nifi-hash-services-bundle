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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestHashUtils {
    @Test
    public void testAdler32() {
        String result = HashUtils.adler32("4BAC2739-3BDD-9777-CE02453256C5", "sample key");
        assertEquals("351011a3", result);
    }

    @Test
    public void testCrc32() {
        String result = HashUtils.crc32("4BAC2739-3BDD-9777-CE02453256C5", "sample key");
        assertEquals("b111d10f", result);
    }

    @Test
    public void testCrc32c() {
        String result = HashUtils.crc32c("4BAC2739-3BDD-9777-CE02453256C5", "sample key");
        assertEquals("811b3682", result);
    }

    @Test
    public void testFarmHashFingerprint64() {
        String result = HashUtils.farmHashFingerprint64("4BAC2739-3BDD-9777-CE02453256C5", "sample key");
        assertEquals("ab9162eb3439a9a3", result);
    }

    @Test
    public void testSha256() {
        String result = HashUtils.sha256("4BAC2739-3BDD-9777-CE02453256C5", "sample key");
        assertEquals("e0017d72714affa14ee435f57450d4364d2161e2f0abf90019964f1263c1d073", result);
    }

    @Test
    public void testSha384() {
        String result = HashUtils.sha384("4BAC2739-3BDD-9777-CE02453256C5", "sample key");
        assertEquals("5a4ff1e6db9cc4859b011a69e32df10d6dec7a3146aee02f9865fea081d2a5016584377956a164579f2a0dba869da78d", result);
    }

    @Test
    public void testSha512() {
        String result = HashUtils.sha512("4BAC2739-3BDD-9777-CE02453256C5", "sample key");
        assertEquals("cd9d9b85532f02fb977a2742d4c0b388aa317007feb8809e6e58059424a0048eaf0e062e32d12510a006409f58ee664d119d12ee0d75e71d1a84b18c574104ef", result);
    }

}
