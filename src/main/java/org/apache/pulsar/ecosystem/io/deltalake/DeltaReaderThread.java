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

import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The delta reader thread class for {@link DeltaLakeConnectorSource}.
 */
public class DeltaReaderThread extends Thread {
    private static final Logger log = LoggerFactory.getLogger(DeltaReaderThread.class);
    private final DeltaLakeConnectorSource source;
    private boolean stopped;
    private ExecutorService parseParquetExecutor;

    public DeltaReaderThread(DeltaLakeConnectorSource source, ExecutorService executor) {
        this.source = source;
        this.stopped = false;
        this.parseParquetExecutor = executor;
    }


    public void run() {
        DeltaReader reader = this.source.reader;
        DeltaCheckpoint checkpoint = source.getMinCheckpoint();
        while (!stopped) {
            try {
                List<DeltaReader.ReadCursor> actionList = reader.getDeltaActionFromSnapShotVersion(
                        checkpoint.getSnapShotVersion(), checkpoint.isFullCopy());

                for (int i = 0; i < actionList.size(); i++) {
                    List<DeltaReader.RowRecordData> rowRecords = reader.readParquetFile(actionList.get(i));
                }
                RecordSchemaBuilder builder = SchemaBuilder
                        .record("test");
                builder.field("test").required().type(SchemaType.STRING);

                GenericSchema<GenericRecord> schema = GenericAvroSchema.of(builder.build(SchemaType.AVRO));

                GenericRecord key = schema.newRecordBuilder().set("test", "foo").build();
                GenericRecord value = schema.newRecordBuilder().set("test", "bar").build();

            } catch (Exception ex) {
                log.error("read data from delta lake  error.", ex);
                close();
            }
        }
    }

    public void close() {
        stopped = true;
    }
}
