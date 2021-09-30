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

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.CommitInfo;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.RemoveFile;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.apache.pulsar.ecosystem.io.deltalake.parquet.ParquetReaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The delta reader for {@link DeltaReaderThread}.
 */
@Data
public class DeltaReader {
    private static final Logger log = LoggerFactory.getLogger(DeltaReader.class);
    public DeltaCheckpoint startCheckpoint;
    public String tablePath;
    public DeltaLog deltaLog;
    public Function<ReadCursor, Boolean> filter;
    public static long topicPartitionNum;
    public ExecutorService executorService;


    public static long getPartitionIdByDeltaPartitionValue(String partitionValue, long topicPartitionNum) {
        return Murmur3_32Hash.getInstance().makeHash(partitionValue.getBytes())
                % topicPartitionNum;
    }

    /**
     * The ActionLog is used to describe one meta action change.
     */
    @Data
    public static class ReadCursor {
        Action act;
        Long version;
        Boolean isFullSnapShot;
        long changeIndex;
        long rowNum;
        boolean endOfFile;
        boolean endOfVersion;
        String partitionValue;

        public ReadCursor(Action act, Long version, Boolean isFullSnapShot,
                          long changeIndex, String partitionValue) {
            new ReadCursor(act, version, isFullSnapShot, changeIndex, partitionValue, 0);
        }
        public ReadCursor(Action act, Long version, Boolean isFullSnapShot,
                          long changeIndex, String partitionValue, long rowNum) {
            this.act = act;
            this.version = version;
            this.isFullSnapShot = isFullSnapShot;
            this.changeIndex = changeIndex;
            this.partitionValue = partitionValue;
            this.rowNum = rowNum;
        }
    }

    /**
     * The RowRecordData is used to contain the data and next read position.
     */
    public static class RowRecordData {
        ReadCursor nextCursor;
        SimpleGroup simpleGroup;

        public RowRecordData(ReadCursor nextCursor, SimpleGroup simpleGroup) {
            this.nextCursor = nextCursor;
            this.simpleGroup = simpleGroup;
        }
    }

    public DeltaReader(DeltaCheckpoint startCheckpoint, String tablePath, long topicPartitionNum) throws Exception {
        new DeltaReader(startCheckpoint, tablePath, topicPartitionNum, null);
    }

    public DeltaReader(DeltaCheckpoint startCheckpoint, String tablePath, long topicPartitionNum,
                       Function<ReadCursor, Boolean> filter) throws Exception {
        this.startCheckpoint = startCheckpoint;
        this.tablePath = tablePath;
        this.topicPartitionNum = topicPartitionNum;
        this.filter = filter;
        this.open();
    }

    public DeltaReader(String tablePath, long topicPartitionNum,
                       Function<ReadCursor, Boolean> filter) throws Exception {
        this.tablePath = tablePath;
        this.filter = filter;
        this.topicPartitionNum = topicPartitionNum;
        this.open();
    }

    public DeltaReader(String tablePath) throws Exception {
        this.tablePath = tablePath;
        this.open();
    }


    public Long getSnapShotVersionFromTimeStamp(Long timeStamp) {
        Snapshot s = null;
        try {
            s = deltaLog.getSnapshotForTimestampAsOf(timeStamp);
            return s.getVersion();
        } catch (Exception e) {
            s = deltaLog.snapshot();
            long v = s.getVersion();
            log.error("timestamp  {} is not exist is delta lake, will use the latest version {}",
                    timeStamp, v);
            return v;
        }
    }

    public Long getAndValidateSnapShotVersion(Long snapShotVersion) {
        Snapshot s = null;
        try {
            if (snapShotVersion == -1) {
                s = deltaLog.snapshot();
                return s.getVersion();
            }
            s = deltaLog.getSnapshotForVersionAsOf(snapShotVersion);
            return s.getVersion();
        } catch (Exception e) {
            s = deltaLog.snapshot();
            long v = s.getVersion();
            log.error("snapShotVersion {} is not exist is delta lake, will use the latest version {}",
                    snapShotVersion, v);
            return v;
        }
    }

    public Long getLatestSnapShotVersion() {
        Snapshot s = deltaLog.snapshot();
        return s.getVersion();
    }

    public List<ReadCursor> getDeltaActionFromSnapShotVersion(Long startVersion,
                                                             boolean isFullSnapshot) throws Exception {
        List<ReadCursor> actionList = new LinkedList<>();
        if (isFullSnapshot) {
            Snapshot snapshot = deltaLog.getSnapshotForVersionAsOf(startVersion);
            List<AddFile> addFiles = snapshot.getAllFiles();
            for (int i = 0; i < addFiles.size(); i++) {
                AddFile add = addFiles.get(i);
                String partitionValue = partitionValueToString(add.getPartitionValues());
                ReadCursor cursor = new ReadCursor(add, startVersion, true, i, partitionValue);
                if (isMatch(cursor)) {
                    actionList.add(cursor);
                }
            }
        } else {
            Iterator<VersionLog> vlogs = deltaLog.getChanges(startVersion, false);
            while (vlogs.hasNext()) {
                VersionLog v = vlogs.next();
                log.info("getChanges return version: {} actionsNum: {}", startVersion, v.getActions().size());
                if (v.getVersion() > startVersion) {
                    continue;
                }
                List<Action> actions = v.getActions();
                for (int i = 0; i < actions.size(); i++) {
                    Action act = actions.get(i);
                    if (act instanceof AddFile) {
                        AddFile addFile = (AddFile) act;
                        String partitionValue = partitionValueToString(addFile.getPartitionValues());
                        ReadCursor cursor = new ReadCursor(addFile, startVersion,
                                false, i, partitionValue);
                        Boolean matchFlag = isMatch(cursor);
                        log.info("getChanges version: {} index: {} addFile {} dataChange {} partitionValue:{} "
                                        + "isMatch:{} modifiTime:{}",
                                startVersion, i, addFile.getPath(),
                                addFile.isDataChange(),
                                partitionValue,
                                matchFlag,
                                addFile.getModificationTime());
                        if (matchFlag) {
                            actionList.add(cursor);
                        }

                    } else if (act instanceof CommitInfo) {
                        CommitInfo info = (CommitInfo) act;
                        log.info("getChanges version: {} index: {} Operation {} "
                                        + "operationParam {} modifiTime:{} operationMetrics:{}",
                                startVersion, i, info.getOperation(),
                                info.getOperationParameters().toString(),
                                info.getTimestamp(),
                                info.getOperationMetrics().get().toString());
                    } else if (act instanceof RemoveFile) {
                        RemoveFile removeFile = (RemoveFile) act;
                        String partitionValue = partitionValueToString(removeFile.getPartitionValues());
                        ReadCursor cursor = new ReadCursor(removeFile, startVersion,
                                false, i, partitionValue);
                        Boolean matchFlag = isMatch(cursor);
                        log.info("getChanges version: {} index: {} removeFile {} dataChange {} partitionValue:{} "
                                        + "isMatch:{} deletionTime:{}",
                                startVersion, i, removeFile.getPath(),
                                removeFile.isDataChange(),
                                partitionValue, matchFlag,
                                removeFile.getDeletionTimestamp());
                        if (matchFlag) {
                            actionList.add(cursor);
                        }
                    } else {
                        Metadata meta = (Metadata) act;
                        log.info("getChanges version: {} index: {} metadataChange schema:{} partitionColum:{}"
                                        + " format:{} createTime:{}",
                                startVersion, i, meta.getSchema().toString(),
                                meta.getPartitionColumns().toString(),
                                meta.getFormat(),
                                meta.getCreatedTime());
                        ReadCursor cursor = new ReadCursor(meta, startVersion,
                                false, i, "");
                        actionList.add(cursor);
                    }
                }
            }
            deltaLog.update();
        }
        return actionList;
    }

    public List<RowRecordData> readParquetFile(ReadCursor startCursor) throws Exception {
        Action act = startCursor.act;
        String path = "";
        List<RowRecordData> recordList = new LinkedList<>();
        if (act instanceof AddFile) {
            CompletableFuture<ParquetReaderUtils.Parquet> parquetFuture =
                    ParquetReaderUtils.getPargetParquetDataquetDataAsync(
                    ((AddFile) act).getPath(), this.executorService);
            ParquetReaderUtils.Parquet parquet = parquetFuture.get();
            for (int i = 0; i < parquet.getData().size(); i++) {
                ReadCursor tmp = startCursor;
                tmp.rowNum = i;
                if (isMatch(tmp)) {
                    recordList.add(new RowRecordData(tmp, parquet.getData().get(i)));
                }
            }
        } else if (act instanceof  RemoveFile) {
            CompletableFuture<ParquetReaderUtils.Parquet> parquetFuture =
                    ParquetReaderUtils.getPargetParquetDataquetDataAsync(
                            ((AddFile) act).getPath(), this.executorService);
            ParquetReaderUtils.Parquet parquet = parquetFuture.get();
            for (int i = 0; i < parquet.getData().size(); i++) {
                ReadCursor tmp = startCursor;
                tmp.rowNum = i;
                if (isMatch(tmp)) {
                    recordList.add(new RowRecordData(tmp, parquet.getData().get(i)));
                }
            }
        } else if (act instanceof CommitInfo) {
            ReadCursor tmp = startCursor;
            recordList.add(new RowRecordData(tmp, null));
        }
        return recordList;
    }

    public static String partitionValueToString(Map<String, String> partitionValue) {
        TreeMap<String, String> treemap = new TreeMap<>(partitionValue);
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry: treemap.entrySet()) {
            builder.append(entry.getKey());
            builder.append("=");
            builder.append(entry.getValue());
        }
        return builder.toString();
    }

    private void open() throws Exception {
        deltaLog = DeltaLog.forTable(new Configuration(), tablePath);
    }

    private boolean isMatch(ReadCursor cursor) {
        if (filter.apply(cursor)) {
            return true;
        }
        return false;
    }
}
