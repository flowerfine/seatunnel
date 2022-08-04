package org.apache.seatunnel.flink.iceberg.sink;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.BaseFlinkSink;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.Map;

@AutoService(BaseFlinkSink.class)
public class IcebergSink implements FlinkStreamSink {

    private Config config;

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
        FlinkStreamSink.super.prepare(env);
    }

    @Override
    public void outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        StreamTableEnvironment tableEnvironment = env.getStreamTableEnvironment();
        Table table = tableEnvironment.fromDataStream(dataStream);

        System.setProperty("aws.region", "us-east-1");
        System.setProperty("aws.accessKeyId", "demo");
        System.setProperty("aws.secretAccessKey", "password");

        Map<String, String> props =
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.ORC.name(),
                        "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                        "s3.endpoint", "http://localhost:9000",
                        "warehouse", "s3://scaleph");

        Configuration conf = new Configuration();
        conf.set("fs.s3a.connection.ssl.enabled", "false");
        conf.set("fs.s3a.endpoint", "http://localhost:9000");
        conf.set("fs.s3a.access.key", "admin");
        conf.set("fs.s3a.secret.key", "password");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.fast.upload", "true");
        CatalogLoader catalog = CatalogLoader.custom("my_catalog", props, conf, "org.apache.iceberg.aws.glue.GlueCatalog");

        TableIdentifier tableIdentifier = TableIdentifier.of("scaleph", "table_test");

        TableLoader tableLoader = TableLoader.fromCatalog(catalog, tableIdentifier);

        FlinkSink.forRow(dataStream, table.getSchema())
                .tableLoader(tableLoader)
                .writeParallelism(1)
                .overwrite(false)
                .append();
    }
}
