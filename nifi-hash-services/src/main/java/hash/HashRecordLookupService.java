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

import com.google.common.hash.Hashing;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

@Tags({ "example"})
@CapabilityDescription("Example ControllerService implementation of MyService.")
public class HashRecordLookupService extends AbstractControllerService implements LookupService<Record> {

    static final Logger LOG = LoggerFactory.getLogger(HashRecordLookupService.class);

    public static final PropertyDescriptor HASH_KEY = new PropertyDescriptor
            .Builder().name("key")
            .displayName("Hash Key")
            .description("Hash Key")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;

    private String hashKey;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HASH_KEY);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        this.hashKey = context.getProperty(HASH_KEY).getValue();
    }

    @OnDisabled
    public void shutdown() {

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

    @Override
    public Set<String> getRequiredKeys() {
        return Collections.emptySet();
    }

    private String getHash(String val) {
        if (StringUtils.isBlank(val)) {
            return val;
        } else {
            String newVal = this.hashKey + val + Base64.getEncoder().encodeToString(val.getBytes());
            return Hashing.sha256().hashString(newVal, StandardCharsets.UTF_8).toString();
        }
    }
}
