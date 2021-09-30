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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A source connector that read data from delta lake through delta standalone reader.
 */
@Getter(AccessLevel.PACKAGE)
public class DeltaLakeConnectorSource implements Source<GenericRecord> {
    private static final Logger log = LoggerFactory.getLogger(DeltaLakeConnectorSource.class);
    private final Integer minCheckpointMapKey = -1;
    public DeltaLakeConnectorConfig config;
    // delta lake schema, when delta lake schema changes,this schema will change
    private SourceContext sourceContext;
    private ExecutorService executor;
    private ExecutorService parseParquetExecutor;
    private LinkedBlockingQueue<DeltaRecord> queue;
    private long topicPartitionNum;
    private final Map<Integer, DeltaCheckpoint> checkpointMap = new HashMap<Integer, DeltaCheckpoint>();
    public DeltaReader reader;

    @Override
    public void open(Map<String, Object> map, SourceContext sourceContext) throws Exception {
        if (null != config) {
            throw new IllegalStateException("Connector is already open");
        }
        this.sourceContext = sourceContext;

        // load the configuration and validate it
        this.config = DeltaLakeConnectorConfig.load(map);
        this.config.validate();

        CompletableFuture<List<String>> listPartitions =
                sourceContext.getPulsarClient().getPartitionsForTopic(sourceContext.getOutputTopic());
        this.topicPartitionNum = listPartitions.get().size();

        // try to open the delta lake
        reader = new DeltaReader(config.tablePath);

        Optional<Map<Integer, DeltaCheckpoint>> checkpointMapOpt = getCheckpointFromStateStore(sourceContext);
        if (!checkpointMapOpt.isPresent()) {
            log.info("instanceId:{} source connector do nothing, without any parition assigned",
                    sourceContext.getInstanceId());
            return;
        }
        reader.setFilter(initDeltaReadFilter(checkpointMap));
        reader.setStartCheckpoint(checkpointMap.get(minCheckpointMapKey));
        DeltaReader.topicPartitionNum = this.topicPartitionNum;
        parseParquetExecutor = Executors.newFixedThreadPool(3);
        reader.setExecutorService(parseParquetExecutor);

        executor = Executors.newFixedThreadPool(1);
        executor.execute(new DeltaReaderThread(this, parseParquetExecutor));

    }

    @Override
    public Record<GenericRecord> read() throws Exception {
        return this.queue.take();
    }

    @Override
    public void close() throws Exception {
    }

    public void enqueue(DeltaReader.RowRecordData rowRecordData) {
        try {
            this.queue.put(new DeltaRecord(rowRecordData));
        } catch (Exception ex) {
            log.error("delta message enqueue interrupted", ex);
        }
    }


    /**
     * get checkpoint position from pulsar function stateStore.
     * @return if this instance not own any partition, will return empty, else return the checkpoint map.
     */
    public Optional<Map<Integer, DeltaCheckpoint>> getCheckpointFromStateStore(SourceContext sourceContext)
            throws Exception {
        int instanceId = sourceContext.getInstanceId();
        int numInstance = sourceContext.getNumInstances();
        DeltaCheckpoint checkpoint = null;

        List<Integer> partitionList = new LinkedList<Integer>();
        for (int i = 0; i < topicPartitionNum; i++) {
            if (i % numInstance == instanceId) {
                if (instanceId > topicPartitionNum) {
                    partitionList.add(i);
                }
            }
        }

        if (partitionList.size() <= 0) {
            return Optional.empty();
        }

        for (int i = 0; i < partitionList.size(); i++) {
            ByteBuffer byteBuffer = null;
            try {
                byteBuffer = sourceContext.getState(DeltaCheckpoint.getStatekey(partitionList.get(i)));
            } catch (Exception e) {
                log.error("getState exception failed, ", e);
                continue;
            }
            String jsonString = Charset.forName("utf-8").decode(byteBuffer).toString();
            ObjectMapper mapper = new ObjectMapper();
            DeltaCheckpoint tmpCheckpoint = null;
            try {
                tmpCheckpoint = mapper.readValue(jsonString, DeltaCheckpoint.class);
            } catch (Exception e) {
                log.error("parse the checkpoint for partition {} failed, jsonString: {}",
                        partitionList.get(i), jsonString);
                throw new Exception("parse checkpoint failed");
            }
            checkpointMap.put(partitionList.get(i), tmpCheckpoint);
            if (checkpoint == null || checkpoint.compareTo(tmpCheckpoint) > 0) {
                checkpoint = tmpCheckpoint;
            }
        }

        if (checkpointMap.size() == 0) {
            Long startVersion = reader.getLatestSnapShotVersion();
            if (!this.config.startingVersion.equals("")) {
                startVersion = reader.getAndValidateSnapShotVersion(this.config.startingSnapShotVersionNumber);
            } else if (!this.config.startingTimeStamp.equals("")) {
                startVersion = reader.getSnapShotVersionFromTimeStamp(this.config.startingTimeStampSecond);
            }
            if (this.config.includeHistoryData) {
                checkpointMap.put(minCheckpointMapKey,
                        new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, startVersion));
            } else {
                checkpointMap.put(minCheckpointMapKey,
                        new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, startVersion));
            }
            return Optional.of(checkpointMap);
        }

        Long version = reader.getAndValidateSnapShotVersion(checkpoint.getSnapShotVersion());
        if (version > checkpoint.getSnapShotVersion()) {
            log.error("checkpoint version: {} not exist, current version {}", checkpoint.getSnapShotVersion(), version);
            throw new Exception("last checkpoint version not exist");
        }
        checkpointMap.put(minCheckpointMapKey, checkpoint);

        return Optional.of(checkpointMap);
    }

    private Function<DeltaReader.ReadCursor, Boolean> initDeltaReadFilter(Map<Integer, DeltaCheckpoint> partitionMap) {
        return (readCursor) -> {
            long slot = DeltaReader.getPartitionIdByDeltaPartitionValue(readCursor.partitionValue,
                    this.topicPartitionNum);
            if (!partitionMap.containsKey(slot)) {
                return false;
            }

            DeltaCheckpoint newCheckPoint = null;
            if (readCursor.isFullSnapShot) {
                newCheckPoint = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, readCursor.version);
            } else {
                newCheckPoint = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, readCursor.version);
            }
            newCheckPoint.setMetadataChangeFileIndex(readCursor.changeIndex);

            DeltaCheckpoint s = this.checkpointMap.get(slot);
            if (s == null) {
                log.error("checkpoint for partition {} is missing");
            }
            if (s != null && newCheckPoint.compareTo(s) >= 0) {
                return true;
            }
            return false;
        };
    }


    public DeltaCheckpoint getMinCheckpoint() {
        return this.checkpointMap.get(minCheckpointMapKey);
    }
}