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
import org.apache.flink.util.OutputTag;
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
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, OutputTag<String> Dirty, OutputTag<String> Late) throws Exception {
//        TODO  1、读取mysql上的取消订单业务数据


    }
}
