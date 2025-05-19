package dwd_app;

import base.BaseApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.FlinkSQLUtil;

/**
 * @基本功能:   交易域——会员开通事实表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-16 23:07:49
 **/

public class Dwd_fact_trade_vip extends BaseApp {

    public static void main(String[] args) throws Exception {
//        启动程序
        new Dwd_fact_trade_vip().start(
                10020,
                4,
                "Dwd_fact_trade_vip"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) throws Exception {
       //        TODO  1、读取mysql上的开通会员业务数据
        tEnv.executeSql("create table order_vip( " +
                "order_id bigint," +
                "user_id bigint," +
                "vip_type string," +
                "order_num int," +
                "channel_id bigint," +
                "status_type_id string," +
                "pay_type string," +
                "amount bigint," +
                "create_ts bigint," +
                "primary key(order_id) not enforced" +
                ")"+FlinkSQLUtil.getMySQLSource("b1uemusic","ods_vip_order")
        );

//        TODO  2、筛选出成功的开通会员订单业务数据
        Table result = tEnv.sqlQuery(
                "select " +
                "order_id," +
                "user_id," +
                "vip_type," +
                "order_num," +
                "channel_id," +
                "pay_type," +
                "amount," +
                "create_ts"+
                " from order_vip " +
                " where status_type_id = '101' ");

//        TODO  3、创建开通会员事实表映射表
         tEnv.executeSql(
                 "create table dwd_fact_trade_order_vip(" +
                "order_id bigint," +
                "user_id bigint," +
                "vip_type string," +
                "order_num int," +
                "channel_id bigint," +
                "pay_type string," +
                "amount bigint," +
                "create_ts bigint," +
                "primary key(order_id) not enforced" +
                ")"+FlinkSQLUtil.getUpsetKafkaDDLSink("BM_DWD_Trade_Order_Vip")
         );

//        TODO  4、将数据输出到kafka上
        result.executeInsert("dwd_fact_trade_order_vip");
    }
}
