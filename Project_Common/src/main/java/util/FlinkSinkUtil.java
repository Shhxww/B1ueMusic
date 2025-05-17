package util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/**
 * @基本功能:   Flink输出工具
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-17 16:32:37
 **/

public class FlinkSinkUtil {

    private static final String kafkaBroker =  "node1:9092,node2:9092,node3:9092";

    /**
     * 获取kafka输出工具
     * @param topic 输出主题
     * @return  kafka输出工具
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

}
