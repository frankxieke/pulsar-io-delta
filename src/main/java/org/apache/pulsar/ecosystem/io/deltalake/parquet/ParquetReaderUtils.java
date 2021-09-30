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

package org.apache.pulsar.ecosystem.io.deltalake.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * The parquet file read util.
 */
public class ParquetReaderUtils {
    /**
     * The parquet file return.
     */
    public static class Parquet {
        private List<SimpleGroup> data;
        private List<Type> schema;
        private MessageType messageType;

        public Parquet(List<SimpleGroup> data, List<Type> schema, MessageType messageType) {
            this.data = data;
            this.schema = schema;
            this.messageType = messageType;
        }

        public MessageType getMessageType() {
            return messageType;
        }

        public List<SimpleGroup> getData() {
            return data;
        }

        public List<Type> getSchema() {
            return schema;
        }
    }


    public static Parquet getPargetParquetDataquetData(String filePath) throws IOException {
            List<SimpleGroup> simpleGroups = new ArrayList<SimpleGroup>();
            Configuration conf = new Configuration();
            ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), conf));
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            List<Type> fields = schema.getFields();
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                long rows = pages.getRowCount();
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

                for (int i = 0; i < rows; i++) {
                    SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                    simpleGroups.add(simpleGroup);
                }
            }
            reader.close();
            return new Parquet(simpleGroups, fields, schema);
    }
    public static CompletableFuture<Parquet> getPargetParquetDataquetDataAsync(String filePath,
                                                                          ExecutorService executorService) {
        CompletableFuture<Parquet> cf = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            try {
                cf.complete(getPargetParquetDataquetData(filePath));
            } catch (IOException e) {
                cf.completeExceptionally(e);
            }
        }, executorService);
        return cf;
    }
}