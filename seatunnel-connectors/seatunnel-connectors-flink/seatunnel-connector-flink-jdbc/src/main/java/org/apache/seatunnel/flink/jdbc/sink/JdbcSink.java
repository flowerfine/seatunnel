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

package org.apache.seatunnel.flink.jdbc.sink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.parsing.StatementSqlBuilder;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSink;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.flink.jdbc.Config.*;

public class JdbcSink implements FlinkStreamSink, FlinkBatchSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSink.class);
    private static final long serialVersionUID = 3677571223952518115L;
    private static final int DEFAULT_BATCH_SIZE = 5000;
    private static final int DEFAULT_MAX_RETRY_TIMES = 3;
    private static final int DEFAULT_INTERVAL_MILLIS = 0;

    private Config config;
    private String driverName;
    private String dbUrl;
    private String username;
    private String password;
    private String query;
    private String statementSql;
    private String[] parameters;
    private String preSql;
    private String postSql;
    private boolean ignorePostSqlExceptions = false;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private long batchIntervalMs = DEFAULT_INTERVAL_MILLIS;
    private int maxRetries = DEFAULT_MAX_RETRY_TIMES;

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
        return CheckConfigUtil.checkAllExists(config, DRIVER, URL, USERNAME, QUERY);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        driverName = config.getString(DRIVER);
        dbUrl = config.getString(URL);
        username = config.getString(USERNAME);
        query = config.getString(QUERY);
        StatementSqlBuilder statementSqlBuilder = new StatementSqlBuilder(query);
        statementSqlBuilder.parse();
        statementSql = statementSqlBuilder.getSql();
        parameters = statementSqlBuilder.getParameters().toArray(new String[0]);
        if (config.hasPath(PASSWORD)) {
            password = config.getString(PASSWORD);
        }
        if (config.hasPath(SINK_BATCH_SIZE)) {
            batchSize = config.getInt(SINK_BATCH_SIZE);
        }
        if (config.hasPath(SINK_BATCH_INTERVAL)) {
            batchIntervalMs = config.getLong(SINK_BATCH_INTERVAL);
        }
        if (config.hasPath(SINK_BATCH_MAX_RETRIES)) {
            maxRetries = config.getInt(SINK_BATCH_MAX_RETRIES);
        }
        if (config.hasPath(SINK_PRE_SQL)) {
            preSql = config.getString(SINK_PRE_SQL);
        }
        if (!env.isStreaming() && config.hasPath(SINK_POST_SQL)) {
            postSql = config.getString(SINK_POST_SQL);
        }
        if (config.hasPath(SINK_IGNORE_POST_SQL_EXCEPTIONS)) {
            ignorePostSqlExceptions = config.getBoolean(SINK_IGNORE_POST_SQL_EXCEPTIONS);
        }
    }

    @Override
    public String getPluginName() {
        return "JdbcSink";
    }

    @Override
    public void outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        executePreSql();

        Table table = env.getStreamTableEnvironment().fromDataStream(dataStream);
        List<TypeInformation> typeInformations = new ArrayList<>();
        Map<String, Integer> fieldTypeMap = JdbcUtil.getFieldTypeMap(table.getSchema(), new HashMap<>());

        SinkFunction<Row> sink = org.apache.flink.connector.jdbc.JdbcSink.sink(
                statementSql,
                (st, row) -> JdbcUtil.setRecordToStatement(st, fieldTypeMap, parameters, row),
                JdbcExecutionOptions.builder()
                        .withBatchSize(batchSize)
                        .withBatchIntervalMs(batchIntervalMs)
                        .withMaxRetries(maxRetries)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(dbUrl)
                        .withDriverName(driverName)
                        .withUsername(username)
                        .withPassword(password)
                        .build());

        if (config.hasPath(PARALLELISM)) {
            dataStream.addSink(sink).setParallelism(config.getInt(PARALLELISM));
        } else {
            dataStream.addSink(sink);
        }
    }

    @Override
    public void outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        executePreSql();
        Table table = env.getBatchTableEnvironment().fromDataSet(dataSet);
        Map<String, TypeInformation> typeInformationMap = new HashMap<>();


        Map<String, Integer> fieldTypeMap = JdbcUtil.getFieldTypeMap(table.getSchema(), typeInformationMap);
        int[] types = JdbcUtil.getSqlTypesArray(fieldTypeMap, parameters);

        TypeInformation[] typeInformations = new TypeInformation[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            typeInformations[i] = typeInformationMap.get(parameters[i]);
        }
        final RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInformations);

        final Map<String, Integer> fieldPositionMap = JdbcUtil.getFieldPositionMap(table.getSchema());

        JdbcOutputFormat format = JdbcOutputFormat.buildJdbcOutputFormat()
                .setDrivername(driverName)
                .setDBUrl(dbUrl)
                .setUsername(username)
                .setPassword(password)
                .setQuery(statementSql)
                .setBatchSize(batchSize)
                .setSqlTypes(types)
                .finish();
        dataSet.map(source -> copyFields(source, fieldPositionMap)).returns(rowTypeInfo).output(format);
    }

    @Override
    public void close() throws Exception {
        executePostSql();
    }

    private Row copyFields(Row source,Map<String, Integer> fieldPositionMap) {
        Row target = new Row(source.getKind(), parameters.length);
        for (int i = 0; i < parameters.length; i++) {
            String parameter = parameters[i];
            int position = fieldPositionMap.get(parameter);
            target.setField(i, source.getField(position));
        }
        return target;
    }

    private void executePreSql() {
        if (StringUtils.isNotBlank(preSql)) {
            LOGGER.info("Starting to execute pre sql: \n {}", preSql);
            try {
                executeSql(preSql);
            } catch (SQLException e) {
                LOGGER.error("Execute pre sql failed, pre sql is : \n {} \n", preSql, e);
                throw new RuntimeException(e);
            }
        }
    }

    private void executePostSql() {
        if (StringUtils.isNotBlank(postSql)) {
            LOGGER.info("Starting to execute post sql: \n {}", postSql);
            try {
                executeSql(postSql);
            } catch (SQLException e) {
                LOGGER.error("Execute post sql failed, post sql is : \n {} \n", postSql, e);
                if (!ignorePostSqlExceptions) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void executeSql(String sql) throws SQLException {
        try (Connection connection = DriverManager.getConnection(dbUrl, username, password);
             Statement statement = connection.createStatement()) {

            statement.execute(sql);
            LOGGER.info("Executed sql successfully.");
        }
    }
}
