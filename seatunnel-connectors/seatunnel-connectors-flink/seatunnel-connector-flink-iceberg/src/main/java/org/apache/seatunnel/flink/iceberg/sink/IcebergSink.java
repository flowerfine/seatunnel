package org.apache.seatunnel.flink.iceberg.sink;

import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.BaseFlinkSink;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.jdbc.JdbcCatalog;

import java.util.HashMap;
import java.util.Map;

@AutoService(BaseFlinkSink.class)
public class IcebergSink implements FlinkStreamSink {

    private Config config;
    private Map<String, String> props;
    private Configuration conf;
    private Catalog catalog;

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

        // must exists but never used
        System.setProperty("aws.region", "us-east-1");
        System.setProperty("aws.accessKeyId", "demo");
        System.setProperty("aws.secretAccessKey", "demo");

        this.props = new HashMap<>();
        props.put(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.ORC.name());
        props.put("s3.endpoint", "http://localhost:9000");
        props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        props.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://scaleph");
        props.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
        props.put(CatalogProperties.URI, "jdbc:mysql://localhost:3306/data_service");
        props.put(JdbcCatalog.PROPERTY_PREFIX + "user", "root");
        props.put(JdbcCatalog.PROPERTY_PREFIX + "password", "123456");

        this.conf = new Configuration();
        conf.set("fs.s3a.connection.ssl.enabled", "false");
        conf.set("fs.s3a.endpoint", "http://localhost:9000");
        conf.set("fs.s3a.access.key", "admin");
        conf.set("fs.s3a.secret.key", "password");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.fast.upload", "true");

        this.catalog = CatalogUtil.buildIcebergCatalog("jdbc_catalog_test", props, conf);
    }

    @Override
    public void outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        StreamTableEnvironment tableEnvironment = env.getStreamTableEnvironment();
        Table table = tableEnvironment.fromDataStream(dataStream);

        TableIdentifier tableIdentifier = TableIdentifier.of("scaleph", "table_test");

        // load catalog
//        CatalogLoader catalogLoader = CatalogLoader.custom("my_catalog", props, conf, JdbcCatalog.class.getName());
//        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);
//        FlinkSink.forRow(dataStream, table.getSchema())
//                .tableLoader(tableLoader)
//                .writeParallelism(1)
//                .overwrite(false)
//                .append();

        // create catalog
        Schema schema = FlinkSchemaUtil.convert(table.getSchema());
        final org.apache.iceberg.Table icebergTable = catalog.createTable(tableIdentifier, schema);
        FlinkSink.forRow(dataStream, table.getSchema())
            .table(icebergTable)
            .writeParallelism(1)
            .overwrite(false)
            .append();
    }
}
