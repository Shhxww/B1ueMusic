package util;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @基本功能:
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-16 16:03:01
 **/

public class FlinkSQLUtil {
    private static String kafkaBroke = "node1:9092,node2:9092,node3:9092";

//    获取mysql-cdc连接器
    public static String getMySQLSource(String database,String table){
        return "WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'node1',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = '000000',\n" +
                "     'database-name' = '"+database+"',\n" +
                "     'table-name' = '"+table+"')";
    }


    /**
     * 获取kafka连接器
     * @param topic 主题
     * @param groupId   消费者组
     * @return  kafka连接器参数
     */
    public static String getKafkaDDLSource(String topic,String groupId){
        return "with(" +
                            "  'connector' = 'kafka'," +
                            "  'properties.group.id' = '" + groupId + "'," +
                            "  'topic' = '" + topic + "'," +
                            "  'properties.bootstrap.servers' = '" + kafkaBroke + "'," +
                            "  'scan.startup.mode' = 'latest-offset'," +
                            "  'json.ignore-parse-errors' = 'true'," + // 当 json 解析失败的时候,忽略这条数据
                            "  'format' = 'json' " +
            ")";
    }

  /**
     * 获取upsert-kafka连接器
     * @param topic 主题
     * @return  upsert-kafka连接器配置
     */
    public static String getUpsetKafkaDDLSink(String topic ){
        return "with(" +
                            "  'connector' = 'upsert-kafka'," +
                            "  'topic' = '" + topic + "'," +
                            "  'properties.bootstrap.servers' = '" + kafkaBroke + "'," +
                            "  'key.json.ignore-parse-errors' = 'true'," +
                            "  'value.json.ignore-parse-errors' = 'true'," +
                            "  'key.format' = 'json', " +
                            "  'value.format' = 'json' " +
                            ")";
    }

    /**
     * 将kafka上ODS的日志数据映射成Ods_log表
     * @param tEnv  流动表环境
     */
    public static void setOds_log(StreamTableEnvironment tEnv){
        tEnv.executeSql("create table Ods_log(" +
                " common map<string, string>," +
                " channel string," +
                " mac_id string," +
                " type string," +
                " ts bigint" +
                ")"+getKafkaDDLSource("BM_log","ods_log")
        );
    }


}
