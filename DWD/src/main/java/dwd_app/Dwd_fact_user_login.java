package dwd_app;

import base.BaseApp;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.FlinkSQLUtil;

/**
 * @基本功能:   用户域——用户登录事实表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-16 22:04:46
 **/

/**
 * 数据样本
      {
          "common":
           {
               "login_id": 120524,
               "user_id": 10021,
               "province_id": 1,
              "login_ts": 1748988324000
              },
          "channel": "APP",
          "mac_id": "532223464136820614",
          "type": "login",
           "ts": 1748988324000
      }
 */

public class Dwd_fact_user_login extends BaseApp {

    public static void main(String[] args) throws Exception {
//        启动程序
        new Dwd_fact_user_login().start(
                10013,
                4,
                "Dwd_fact_user_login"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) throws Exception {
//        TODO  1、创建日志数据映射表
        FlinkSQLUtil.setOds_log(tEnv);

//        TODO  2、过滤出用户登录日志数据，并进行清洗
        Table result = tEnv.sqlQuery("" +
                "select " +
                "cast(common['login_id'] as bigint ) login_id," +
                "cast(common['user_id'] as bigint ) user_id," +
                "cast(common['province_id'] as int ) province_id," +
                "channel," +
                "ts " +
                "from Ods_log " +
                "where type = 'login' " +
                "   and cast(common['login_id'] as bigint )>0 " +
                "   and cast(common['user_id'] as bigint )>0 " +
                "   and  cast(common['province_id'] as bigint ) between 1 and 34"
        );

//        TODO  3、创建用户登录事实表的映射表
         tEnv.executeSql("create table dwd_fact_user_login(" +
                " login_id bigint," +
                " user_id bigint," +
                " province_id int," +
                " channel string," +
                " ts bigint," +
                "primary key(login_id) not enforced" +
                ")"+FlinkSQLUtil.getUpsetKafkaDDLSink("BM_DWD_USER")
        );

//        TODO  4、将清洗后的数据插入到映射表中去
        result.executeInsert("dwd_fact_user_login");
    }
}
