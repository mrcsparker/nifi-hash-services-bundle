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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.util.*;

@Tags({ "hash"})
@CapabilityDescription("Hash Lookup Service")
public class HashRecordLookupService extends AbstractHashLookupService<Record> {

    private final List<PropertyDescriptor> propertyDescriptors;

    public HashRecordLookupService() {
        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(HASH_KEY);
        propertyDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException {

        List<RecordField> fields = new ArrayList<>();
        Map<String, Object> res = new HashMap<>();

        for (String column : coordinates.keySet()) {
            fields.add(new RecordField(column, RecordFieldType.STRING.getDataType()));
            res.put(column, getHash(column));
        }

        return Optional.of(new MapRecord(new SimpleRecordSchema(fields), res));
    }

    @Override
    public Class<?> getValueType() {
        return Record.class;
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to initialize HashRecordLookupService
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        this.hashKey = context.getProperty(HASH_KEY).getValue();
    }
}
