package util;

import com.google.gson.JsonObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.util.Properties;

/**
 * @基本功能:
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-13 16:11:04
 **/

public class FlinkSourceUtil {
//    获取Kafka连接
    public static KafkaSource<String> getkafkaSource(String topic, String groupId) {
        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers("node1:9092")
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
 //                    过滤空数据
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null) {
                            return new String(message);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();

        return kafkaSource;
    }
//    FlinkCDC连接MySQL
    public static MySqlSource<String> getMySqlSource(String database, String tablename) {
        MySqlSource<String> mySqlSource =
                new MySqlSourceBuilder<String>().
                        hostname("node1")
                        .port(3306)
                        .username("root")
                        .password("000000")
                        .databaseList(database)
                        .tableList(database + "." + tablename)
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();
        return mySqlSource;
    }

}
