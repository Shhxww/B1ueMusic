package dwd_app;

import base.BaseApp;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.FlinkSQLUtil;

/**
 * @基本功能:   用户域——用户注册事实表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-16 15:25:27
 **/

/** 数据样本
    {
        "common": {
             "register_id": 971454,
             "user_id": 10059,
             "province_id": 8,
             "channel_ts": 1748530369000
            },
         "channel": "APP",
         "mac_id": "419243495089488208",
         "type": "register",
         "ts": 1748530369000
    }
 **/

public class Dwd_fact_user_register extends BaseApp {

    public static void main(String[] args) throws Exception {
//        启动程序
        new Dwd_fact_user_register().start(
                10012,
                4,
                "Dwd_fact_user_register"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) throws Exception {
//        TODO  1、创建日志数据映射表
        FlinkSQLUtil.setOds_log(tEnv);


//        TODO  2、过滤出用户注册日志数据，并进行清洗
    Table result = tEnv.sqlQuery(
            "select " +
            "   cast(common['register_id'] as bigint) as register_id," +
            "   cast(common['user_id'] as bigint) as user_id," +
            "   cast(common['province_id'] as int) as province_id," +
            "   channel," +
            "   ts " +
            "from Ods_log " +
            "where type='register' " +
            "   and cast(common['user_id'] as bigint) > 0 " +
            "   and cast(common['province_id'] as int) between 1 and 34"
        );

//        TODO  3、创建用户注册事实表的映射表
        tEnv.executeSql("create table dwd_fact_user_register(" +
                " register_id bigint," +
                " user_id bigint," +
                " province_id int," +
                " channel string," +
                " ts bigint," +
                "primary key(register_id) not enforced" +
                ")"+FlinkSQLUtil.getUpsetKafkaDDLSink("BM_DWD_USER")
        );

//        TODO  4、将清洗后的数据插入到映射表中去
        result.executeInsert("dwd_fact_user_register");
    }
}
