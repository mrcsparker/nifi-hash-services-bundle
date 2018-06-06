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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Supplier;

public class HashUtils {

    public static AllowableValue HASH_ADLER32 = new AllowableValue("hash-adler32",
            "adler32",
            "A hash function implementing the Adler-32 checksum algorithm (32 hash bits).");

    public static AllowableValue HASH_CRC32 = new AllowableValue("hash-crc32",
            "crc32",
            "A hash function implementing the CRC-32 checksum algorithm (32 hash bits).");

    public static AllowableValue HASH_CRC32C = new AllowableValue("hash-crc32c",
            "crc32c",
            "A hash function implementing the CRC32C checksum algorithm (32 hash bits) as described by RFC 3720, Section 12.1.");

    public static AllowableValue HASH_FARMHASHFINGERPRINT64 = new AllowableValue("hash-farmHashFingerprint64",
            "FarmHash's Fingerprint64",
            "A hash function implementing FarmHash's Fingerprint64, an open-source algorithm.");

    public static AllowableValue HASH_SHA256 = new AllowableValue("hash-sha256",
        "sha256",
        "A hash function implementing the SHA-256 algorithm (256 hash bits).");

    public static AllowableValue HASH_SHA384 = new AllowableValue("hash-sha384",
            "sha384",
            "A hash function implementing the SHA-384 algorithm (384 hash bits).");

    public static AllowableValue HASH_SHA512 = new AllowableValue("hash-sha512",
            "sha512",
            "A hash function implementing the SHA-512 algorithm (512 hash bits).");

    static final PropertyDescriptor HASH_ALGORITHM = new PropertyDescriptor.Builder()
            .name("hash-algorithm")
            .displayName("Hash Algorithm")
            .description("Specifies which hash algorithm to use")
            .allowableValues(HASH_ADLER32, HASH_CRC32, HASH_CRC32C, HASH_FARMHASHFINGERPRINT64, HASH_SHA256, HASH_SHA384, HASH_SHA512)
            .defaultValue(HASH_SHA256.getValue())
            .expressionLanguageSupported(false)
            .required(true)
            .build();

    static String adler32(String hash, String val) {
        return doHash(Hashing::adler32, hash, val);
    }

    static String crc32(String hash, String val) {
        return doHash(Hashing::crc32, hash, val);
    }

    static String crc32c(String hash, String val) {
        return doHash(Hashing::crc32c, hash, val);
    }

    static String farmHashFingerprint64(String hash, String val) {
        return doHash(Hashing::farmHashFingerprint64, hash, val);
    }

    static String sha256(String hash, String val) {
        return doHash(Hashing::sha256, hash, val);
    }

    static String sha384(String hash, String val) {
        return doHash(Hashing::sha384, hash, val);
    }

    static String sha512(String hash, String val) {
        return doHash(Hashing::sha512, hash, val);
    }

    private static String doHash(Supplier<HashFunction> f, String hash, String val) {
        if (StringUtils.isBlank(val)) {
            return val;
        } else {
            String newVal = hash + val + Base64.getEncoder().encodeToString(val.getBytes());
            return f.get().hashString(newVal, StandardCharsets.UTF_8).toString();
        }
    }

    static String getHash(String hashAlgorithm, String hash, String val) {

        if (hashAlgorithm == null || StringUtils.isBlank(val)) {
            return val;
        }

        if (hashAlgorithm.equals(HashUtils.HASH_ADLER32.getValue())) {
            return HashUtils.adler32(hash, val);
        } else if (hashAlgorithm.equals(HashUtils.HASH_CRC32.getValue())) {
            return HashUtils.adler32(hash, val);
        } else if (hashAlgorithm.equals(HashUtils.HASH_CRC32C.getValue())) {
            return HashUtils.crc32c(hash, val);
        } else if (hashAlgorithm.equals(HashUtils.HASH_FARMHASHFINGERPRINT64.getValue())) {
            return HashUtils.farmHashFingerprint64(hash, val);
        } else if (hashAlgorithm.equals(HashUtils.HASH_SHA256.getValue())) {
            return HashUtils.sha256(hash, val);
        } else if (hashAlgorithm.equals(HashUtils.HASH_SHA384.getValue())) {
            return HashUtils.sha384(hash, val);
        } else if (hashAlgorithm.equals(HashUtils.HASH_SHA512.getValue())) {
            return HashUtils.sha512(hash, val);
        }

        return HashUtils.sha256(hash, val);
    }
}
