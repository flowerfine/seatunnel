/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.flink.hudi.sink;

import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.BaseFlinkSink;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.utils.Pipelines;

@AutoService(BaseFlinkSink.class)
public class HudiSink implements FlinkStreamSink {

    private Config config;
    private Configuration conf;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return FlinkStreamSink.super.checkConfig();
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        this.conf = env.getStreamTableEnvironment().getConfig().getConfiguration();
        conf.setString(FlinkOptions.PATH, "s3a://scaleph/hudi/test");
        conf.setString(FlinkOptions.TABLE_NAME, "test");
        conf.setString(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_COPY_ON_WRITE);
        conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, 1000L);

        // 在 core-site.xml 中添加 s3 配置
        // https://hudi.apache.org/cn/docs/s3_hoodie#aws-credentials
        // fs.s3a.endpoint = http://localhost:9000
        // fs.s3a.access.key = admin
        // fs.s3a.secret.key = password
        // fs.s3a.path.style.access = true
    }

    @Override
    public void outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        final StreamTableEnvironment tableEnvironment = env.getStreamTableEnvironment();
        final Table table = tableEnvironment.fromDataStream(dataStream);

        final ResolvedSchema schema = table.getResolvedSchema();
        RowType rowType = (RowType) schema.toSinkRowDataType().notNull().getLogicalType();
        Pipelines.append(conf, rowType, dataStream.map(this::convert), false);
    }

    private RowData convert(Row row) {
        GenericRowData rowData = new GenericRowData(row.getKind(), row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
            rowData.setField(i, row.getField(i));
        }
        return rowData;
    }
}
