/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.deltalake;

import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.RemoveFile;
import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.ByteType;
import io.delta.standalone.types.DateType;
import io.delta.standalone.types.DecimalType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.NullType;
import io.delta.standalone.types.ShortType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.TimestampType;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.FieldSchemaBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;

/**
 * A record wrapping an dataChange or metaChange message.
 */
@Data
public class DeltaRecord implements Record<GenericRecord> {
    public static final String OP_FIELD = "op";
    public static final String PARTITION_VALUE_FIELD = "partition_value";
    public static final String CAPTURE_TS_FIELD = "capture_ts";
    public static final String TS_FIELD = "ts";

    public static final String OP_ADD_RECORD = "c";
    public static final String OP_DELETE_RECORD = "r";
    public static final String OP_META_SCHEMA = "m";

    private Map<String, String> properties;
    private GenericRecord value;
    private static GenericSchema<GenericRecord> s;
    private static StructType deltaSchema;


    public DeltaRecord(DeltaReader.RowRecordData rowRecordData) throws Exception {
        properties = new HashMap<>();
        if (rowRecordData.nextCursor.act instanceof AddFile) {
            properties.put(OP_FIELD, OP_ADD_RECORD);
            properties.put(PARTITION_VALUE_FIELD, DeltaReader.
                    partitionValueToString(((AddFile) rowRecordData.nextCursor.act).getPartitionValues()));
            properties.put(CAPTURE_TS_FIELD, Long.toString(System.currentTimeMillis() / 1000));
            properties.put(TS_FIELD, Long.toString(((AddFile) rowRecordData.nextCursor.act).getModificationTime()));
            value = getGenericRecord(deltaSchema, rowRecordData);

        } else if (rowRecordData.nextCursor.act instanceof RemoveFile) {
            properties.put(OP_FIELD, OP_DELETE_RECORD);
            properties.put(PARTITION_VALUE_FIELD, DeltaReader.
                    partitionValueToString(((RemoveFile) rowRecordData.nextCursor.act).getPartitionValues()));
            properties.put(CAPTURE_TS_FIELD,  Long.toString(System.currentTimeMillis() / 1000));
            properties.put(TS_FIELD, Long.toString(((RemoveFile) rowRecordData.nextCursor.act).
                    getDeletionTimestamp().get()));
            value = getGenericRecord(deltaSchema, rowRecordData);
        } else {
            properties.put(OP_FIELD, OP_META_SCHEMA);
            deltaSchema = ((Metadata) rowRecordData.nextCursor.act).getSchema();
            s = convertToPulsarSchema(deltaSchema);
        }

    }

    private GenericSchema<GenericRecord> convertToPulsarSchema(StructType parquetSchema) throws Exception {
        // Try to transform schema
        RecordSchemaBuilder builder = SchemaBuilder
                .record(parquetSchema.getTypeName());
        FieldSchemaBuilder fbuilder = null;
        for (int i = 0; i < parquetSchema.getFields().length; i++) {
            StructField field = parquetSchema.getFields()[i];
            boolean required = field.isNullable();
            fbuilder = builder.field(field.getName());
            if (required){
                fbuilder = fbuilder.required();
            } else {
                fbuilder = fbuilder.optional();
            }
            if (field.getDataType() instanceof StringType) {
                fbuilder = fbuilder.type(SchemaType.STRING);
            } else if (field.getDataType() instanceof BooleanType) {
                fbuilder = fbuilder.type(SchemaType.BOOLEAN);
            } else if (field.getDataType() instanceof BinaryType) {
                fbuilder = fbuilder.type(SchemaType.BYTES);
            } else if (field.getDataType() instanceof DoubleType) {
                fbuilder = fbuilder.type(SchemaType.DOUBLE);
            } else if (field.getDataType() instanceof FloatType) {
                fbuilder = fbuilder.type(SchemaType.FLOAT);
            } else if (field.getDataType() instanceof IntegerType) {
                fbuilder = fbuilder.type(SchemaType.INT32);
            } else if (field.getDataType() instanceof LongType) {
                fbuilder = fbuilder.type(SchemaType.INT64);
            } else if (field.getDataType() instanceof ShortType) {
                fbuilder = fbuilder.type(SchemaType.INT16);
            } else if (field.getDataType() instanceof ByteType) {
                fbuilder = fbuilder.type(SchemaType.INT8);
            } else if (field.getDataType() instanceof NullType) {
                fbuilder = fbuilder.type(SchemaType.NONE);
            } else if (field.getDataType() instanceof DateType) {
                fbuilder = fbuilder.type(SchemaType.DATE);
            } else if (field.getDataType() instanceof TimestampType) {
                fbuilder = fbuilder.type(SchemaType.TIMESTAMP);
            } else if (field.getDataType() instanceof DecimalType) {
                fbuilder = fbuilder.type(SchemaType.DOUBLE);
            } else { // not support other data type
                fbuilder = fbuilder.type(SchemaType.STRING);
            }
        }

        if (fbuilder == null) {
            throw new Exception("filed is empty, can not covert to pulsar schema");
        }
        GenericSchema<GenericRecord> schema1 = GenericAvroSchema.of(builder.build(SchemaType.AVRO));
        return schema1;
    }

    private GenericRecord getGenericRecord(StructType rowRecordType, DeltaReader.RowRecordData rowRecordData) {
        GenericRecordBuilder builder = s.newRecordBuilder();
        for (int i = 0; i < rowRecordType.getFields().length; i++) {
            StructField field = rowRecordType.getFields()[i];
            Object value;
            if (field.getDataType() instanceof StringType) {
                value = rowRecordData.simpleGroup.getString(i, 0);
            } else if (field.getDataType() instanceof BooleanType) {
                value = rowRecordData.simpleGroup.getBoolean(i, 0);
            } else if (field.getDataType() instanceof BinaryType) {
                value = rowRecordData.simpleGroup.getBinary(i, 0);
            } else if (field.getDataType() instanceof DoubleType) {
                value = rowRecordData.simpleGroup.getDouble(i, 0);
            } else if (field.getDataType() instanceof FloatType) {
                value = rowRecordData.simpleGroup.getFloat(i, 0);
            } else if (field.getDataType() instanceof IntegerType) {
                value = rowRecordData.simpleGroup.getInteger(i, 0);
            } else if (field.getDataType() instanceof LongType) {
                value = rowRecordData.simpleGroup.getLong(i, 0);
            } else if (field.getDataType() instanceof ShortType) {
                value = rowRecordData.simpleGroup.getInteger(i, 0);
            } else if (field.getDataType() instanceof ByteType) {
                value = rowRecordData.simpleGroup.getInteger(i, 0);
            } else if (field.getDataType() instanceof NullType) {
                value = rowRecordData.simpleGroup.getInteger(i, 0);
            } else if (field.getDataType() instanceof DateType) {
                value = rowRecordData.simpleGroup.getTimeNanos(i, 0);
            } else if (field.getDataType() instanceof TimestampType) {
                value = rowRecordData.simpleGroup.getTimeNanos(i, 0);
            } else if (field.getDataType() instanceof DecimalType) {
                value = rowRecordData.simpleGroup.getDouble(i, 0);
            } else { // not support other data type
                value = rowRecordData.simpleGroup.getValueToString(i, 0);
            }
            builder.set(field.getName(), value);
        }
        return builder.build();

    }

    @Override
    public Optional<String> getTopicName() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getKey() {
        return Optional.empty();
    }

    @Override
    public Schema<GenericRecord> getSchema() {
        return s;
    }

    @Override
    public GenericRecord getValue() {
        return value;
    }

    @Override
    public Optional<Long> getEventTime() {
        try {
            Long s = Long.parseLong(properties.get(TS_FIELD));
            return Optional.of(s);
        } catch (Exception e) {
            return Optional.of(new Long(0));
        }
    }

    @Override
    public Optional<String> getPartitionId() {
        return Optional.empty();
    }

    @Override
    public Optional<Integer> getPartitionIndex() {
        Integer partitionId = 0;
        String partitionValueStr = properties.get(PARTITION_VALUE_FIELD);
        return Optional.of(new Integer((int) DeltaReader.getPartitionIdByDeltaPartitionValue(partitionValueStr,
                DeltaReader.topicPartitionNum)));
    }

    @Override
    public Optional<Long> getRecordSequence() {
        return Optional.empty();
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void ack() {

    }

    @Override
    public void fail() {

    }

    @Override
    public Optional<String> getDestinationTopic() {
        return Optional.empty();
    }

    @Override
    public Optional<Message<GenericRecord>> getMessage() {
        return Optional.empty();
    }
}
