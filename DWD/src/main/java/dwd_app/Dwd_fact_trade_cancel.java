package dwd_app;

import base.BaseApp;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.FlinkSQLUtil;
import util.FlinkSourceUtil;

/**
 * @基本功能:
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-19 17:28:26
 **/

// {"op":"r","after":{"cancel_reason":"订单信息错误","order_cancel_id":40002,"cancel_ts":1675987200,"order_id":20037,"order_type":1,"status":"103"},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"b1uemusic","table":"ods_cancel_order"},"ts_ms":1747653312393}


public class Dwd_fact_trade_cancel extends BaseApp {

    public static void main(String[] args) throws Exception {
        new Dwd_fact_trade_cancel().start(
                10019,
                4,
                "Dwd_fact_trade_cancel"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) throws Exception {
//        TODO  1、读取mysql上的取消订单业务数据
        tEnv.executeSql("create table order_cancel( " +
                "order_cancel_id bigint," +
                "order_id bigint," +
                "order_type int," +
                "cancel_reason string," +
                "status string," +
                "cancel_ts bigint," +
                "primary key(order_cancel_id) not enforced" +
                ")"+FlinkSQLUtil.getMySQLSource("b1uemusic","ods_cancel_order")
        );

//        TODO  2、筛选出成功的取消订单业务数据
        Table result = tEnv.sqlQuery(
                "select " +
                " order_cancel_id," +
                " order_id," +
                " order_type," +
                " cancel_reason," +
                " cancel_ts " +
                " from order_cancel " +
                " where status = '101' ");

//        TODO  3、创建取消订单事实表映射表
         tEnv.executeSql(
                 "create table dwd_fact_trade_cancel(" +
                "order_cancel_id bigint," +
                "order_id bigint," +
                "order_type int," +
                "cancel_reason string," +
                "cancel_ts bigint," +
                 "primary key(order_cancel_id) not enforced" +
                ")"+FlinkSQLUtil.getUpsetKafkaDDLSink("BM_DWD_Trade_Order_Cancel")
         );

//        TODO  4、将数据输出到kafka上
        result.executeInsert("dwd_fact_trade_cancel");

    }
}
