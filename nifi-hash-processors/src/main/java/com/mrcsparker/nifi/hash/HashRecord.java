package com.mrcsparker.nifi.hash;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathPropertyNameValidator;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HashRecord extends AbstractRecordProcessor  {

    static final Logger LOG = LoggerFactory.getLogger(HashRecord.class);

    private String hashKey;
    private String hashAlgorithm;

    private volatile RecordPathCache recordPathCache;
    private volatile List<String> recordPaths;

    static final PropertyDescriptor HASH_KEY = new PropertyDescriptor.Builder()
            .name("key")
            .displayName("Hash Key")
            .description("Hash Key")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(HASH_KEY);
        properties.add(HashUtils.HASH_ALGORITHM);

        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Specifies the value to use to replace fields in the record that match the RecordPath: " + propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .expressionLanguageSupported(true)
                .addValidator(new RecordPathPropertyNameValidator())
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final boolean containsDynamic = validationContext.getProperties().keySet().stream()
                .anyMatch(PropertyDescriptor::isDynamic);

        if (containsDynamic) {
            return Collections.emptyList();
        }

        return Collections.singleton(new ValidationResult.Builder()
                .subject("User-defined Properties")
                .valid(false)
                .explanation("At least one RecordPath must be specified")
                .build());
    }


    @OnScheduled
    public void createRecordPaths(final ProcessContext context) {
        recordPathCache = new RecordPathCache(context.getProperties().size() * 2);

        final List<String> recordPaths = new ArrayList<>(context.getProperties().size() - 2);
        for (final PropertyDescriptor property : context.getProperties().keySet()) {
            if (property.isDynamic()) {
                recordPaths.add(property.getName());
            }
        }

        this.recordPaths = recordPaths;
    }

    @Override
    protected Record process(Record record, RecordSchema writeSchema, FlowFile flowFile, ProcessContext context) {

        hashKey = context.getProperty(HASH_KEY).getValue();
        hashAlgorithm = context.getProperty(HashUtils.HASH_ALGORITHM).getValue();
        if (hashAlgorithm.isEmpty()) {
            hashAlgorithm = HashUtils.HASH_SHA256.getValue();
        }

        // Incorporate the RecordSchema that we will use for writing records into the Schema that we have
        // for the record, because it's possible that the updates to the record will not be valid otherwise.
        record.incorporateSchema(writeSchema);

        for (final String recordPathText : recordPaths) {
            final RecordPath recordPath = recordPathCache.getCompiled(recordPathText);
            final RecordPathResult result = recordPath.evaluate(record);

            final String replacementValue = context.getProperty(recordPathText).evaluateAttributeExpressions(flowFile).getValue();
            final RecordPath replacementRecordPath = recordPathCache.getCompiled(replacementValue);

            // If we have an Absolute RecordPath, we need to evaluate the RecordPath only once against the Record.
            // If the RecordPath is a Relative Path, then we have to evaluate it against each FieldValue.
            if (replacementRecordPath.isAbsolute()) {
                record = processAbsolutePath(replacementRecordPath, result.getSelectedFields(), record);
            } else {
                record = processRelativePath(replacementRecordPath, result.getSelectedFields(), record);
            }

        }

        return record;

    }

    private Record processAbsolutePath(final RecordPath replacementRecordPath, final Stream<FieldValue> destinationFields, final Record record) {
        final RecordPathResult replacementResult = replacementRecordPath.evaluate(record);
        final List<FieldValue> selectedFields = replacementResult.getSelectedFields().collect(Collectors.toList());
        final List<FieldValue> destinationFieldValues = destinationFields.collect(Collectors.toList());

        return updateRecord(destinationFieldValues, selectedFields, record);
    }

    private Record processRelativePath(final RecordPath replacementRecordPath, final Stream<FieldValue> destinationFields, Record record) {
        final List<FieldValue> destinationFieldValues = destinationFields.collect(Collectors.toList());

        for (final FieldValue fieldVal : destinationFieldValues) {
            final RecordPathResult replacementResult = replacementRecordPath.evaluate(record, fieldVal);
            final List<FieldValue> selectedFields = replacementResult.getSelectedFields().collect(Collectors.toList());
            final Object replacementObject = getReplacementObject(selectedFields);
            fieldVal.updateValue(replacementObject);

            record = updateRecord(destinationFieldValues, selectedFields, record);
        }

        return record;
    }

    private Record updateRecord(final List<FieldValue> destinationFields, final List<FieldValue> selectedFields, final Record record) {
        if (destinationFields.size() == 1 && !destinationFields.get(0).getParentRecord().isPresent()) {
            final Object replacement = getReplacementObject(selectedFields);
            if (replacement == null) {
                return record;
            }
            if (replacement instanceof Record) {
                return (Record) replacement;
            }

            final List<RecordField> fields = selectedFields.stream().map(FieldValue::getField).collect(Collectors.toList());
            final RecordSchema schema = new SimpleRecordSchema(fields);
            final Record mapRecord = new MapRecord(schema, new HashMap<>());
            for (final FieldValue selectedField : selectedFields) {
                mapRecord.setValue(selectedField.getField().getFieldName(), selectedField.getValue());
            }

            return mapRecord;
        } else {
            for (final FieldValue fieldVal : destinationFields) {
                fieldVal.updateValue(getReplacementObject(selectedFields));
            }
            return record;
        }
    }

    private Object getReplacementObject(final List<FieldValue> selectedFields) {
        if (selectedFields.size() > 1) {
            final List<RecordField> fields = selectedFields.stream().map(FieldValue::getField).collect(Collectors.toList());
            final RecordSchema schema = new SimpleRecordSchema(fields);
            final Record record = new MapRecord(schema, new HashMap<>());
            for (final FieldValue fieldVal : selectedFields) {
                record.setValue(fieldVal.getField().getFieldName(), fieldVal.getValue());
            }

            return record;
        }

        if (selectedFields.isEmpty()) {
            return null;
        } else if (selectedFields.get(0).getValue() == null) {
            return null;
        } else {
            String clearTextValue = selectedFields.get(0).getValue().toString();
           return HashUtils.getHash(hashAlgorithm, hashKey, clearTextValue);
        }
    }

}
