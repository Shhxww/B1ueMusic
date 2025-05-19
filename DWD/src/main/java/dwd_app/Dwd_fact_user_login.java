package dwd_app;

import base.BaseApp;
import com.alibaba.fastjson.JSONObject;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.FlinkSQLUtil;
import util.FlinkSinkUtil;
import util.FlinkSourceUtil;

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
        //        TODO  1、读取日志数据并转化为 jsonObject
        SingleOutputStreamOperator<JSONObject> jsonObj = FlinkSourceUtil.getOdsLog(env,"Dwd_fact_user_login");

//        TODO  2、过滤出用户登录日志数据
        SingleOutputStreamOperator<JSONObject> process = jsonObj.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (value != null && value.getString("type").equals("login"))
                    out.collect(value);
            }});

//        TODO  3、对数据进行清洗，将脏数据输出到侧道
        OutputTag<String> Dirty = new OutputTag<String>("BM_Dirty") {};

        SingleOutputStreamOperator<String> result = process.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                try {

                    String channel = value.getString("channel");
                    JSONObject data = value.getJSONObject("common");

                    Long provinceId = data.getLong("province_id");
                    Long userId = data.getLong("user_id");
                    Long loginId = data.getLong("login_id");

                    if (
                            userId > 0L && loginId > 0L && (provinceId > 0 && provinceId < 35)
                    ) {
                        data.put("channel", channel);
                        out.collect(data.toJSONString());
                    } else {
//                        类型数值不符合标准
                        value.put("dirty_type", "1");
                        ctx.output(Dirty, value.toJSONString());
                    }

                } catch (Exception e) {
//                    类型转化错误
                    value.put("dirty_type", "2");
                    ctx.output(Dirty, value.toJSONString());
                }

            }
        });

//        TODO  4、将数据输出到kafka上
        result.sinkTo(FlinkSinkUtil.getKafkaSink("BM_DWD_User_Login"));

//        TODO  5、将脏数据输出到kafka上备用
        result.getSideOutput(Dirty).sinkTo(FlinkSinkUtil.getKafkaSink("BM_Dirty"));

//        TODO  6、启动程序
        env.execute("用户登录事实表");
    }
}
