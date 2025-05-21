package util;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Properties;

/**
 * @基本功能:   Flink输出工具
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-17 16:32:37
 **/

public class FlinkSinkUtil {

    private static final String kafkaBroker =  "node1:9092,node2:9092,node3:9092";

    /**
     * 获取kafka输出类
     * @param topic 输出主题
     * @return  kafka输出类
     */
    public static KafkaSink<String> getKafkaSink(String topic){
        return KafkaSink
                .<String>builder()
                .setBootstrapServers(kafkaBroker)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                        .<String>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build() )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(topic+"_")
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();
    }

    /**
     * 获取Doris输出类
     * @param databaseTable 目标表名
     * @return Doris输出类
     */
    public static DorisSink<String> getDorisSink(String databaseTable) {
    Properties props = new Properties();
    props.setProperty("format", "json");
    props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据
    return DorisSink.<String>builder()
            .setDorisReadOptions(DorisReadOptions.builder().build())
            .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                    .setFenodes("node1:7030")
                    .setTableIdentifier(databaseTable)
                    .setUsername("root")
                    .setPassword("000000")
                    .build()
            )
            .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
//                    .setLabelPrefix()  // stream-load 导入数据时 label 的前缀
//                    .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                    .setBufferCount(3) // 批次条数: 默认 3
                    .setBufferSize(1024 * 1024) // 批次大小: 默认 1M
                    .setCheckInterval(3000) // 批次输出间隔  上述三个批次的限制条件是或的关系
                    .setMaxRetries(3)
                    .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,需要改成 json
                    .build())
            .setSerializer(new SimpleStringSerializer())
            .build();
    }




}
