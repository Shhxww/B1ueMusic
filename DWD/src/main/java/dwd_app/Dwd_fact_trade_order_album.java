package dwd_app;

import base.BaseApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.FlinkSQLUtil;

/**
 * @基本功能:   交易域——专辑购买事实表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-19 21:17:34
 **/

public class Dwd_fact_trade_order_album extends BaseApp {

    public static void main(String[] args) throws Exception {
        new Dwd_fact_trade_order_album().start(
                10021,
                4,
                "Dwd_fact_trade_order_ablum"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) throws Exception {
//        TODO  1、从MySQL读取专辑购买业务数据
        tEnv.executeSql("create table order_album (" +
                "   order_id bigint," +
                "    user_id bigint," +
                "    album_id bigint," +
                "    order_num int," +
                "    channel_id bigint," +
                "    status_type_id string," +
                "    pay_type string," +
                "    amount bigint," +
                "    create_ts bigint," +
                "primary key (order_id) not enforced" +
                ")"+ FlinkSQLUtil.getMySQLSource("b1uemusic","ods_album_order")
        );

//        TODO  2、筛选出成功的订单业务数据
        Table result = tEnv.sqlQuery(
                "select " +
                "order_id," +
                "user_id," +
                "album_id," +
                "order_num," +
                "channel_id," +
                "pay_type," +
                "amount," +
                "create_ts"+
                " from order_album " +
                " where status_type_id = '101' ");

//        TODO  3、创建专辑购买事实表映射表
         tEnv.executeSql(
                 "create table dwd_fact_trade_order_album(" +
                "   order_id bigint," +
                "    user_id bigint," +
                "    album_id bigint," +
                "    order_num int," +
                "    channel_id bigint," +
                "    pay_type string," +
                "    amount bigint," +
                "    create_ts bigint," +
                "primary key (order_id) not enforced" +
                ")"+FlinkSQLUtil.getUpsetKafkaDDLSink("BM_DWD_Trade_Order_Album")
         );

//        TODO  4、将数据输出到kafka上
        result.executeInsert("dwd_fact_trade_order_album");
    }
}
