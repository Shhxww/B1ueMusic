package dwd_app;

import base.BaseApp;
import com.alibaba.fastjson.JSONObject;
import com.sun.xml.internal.bind.v2.TODO;
import function.AsyncDimFunction;
import function.DimAssFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.FlinkSQLUtil;
import util.FlinkSinkUtil;
import util.FlinkSourceUtil;

import java.util.concurrent.TimeUnit;

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
//        TODO  1、读取日志数据并转化为 jsonObject
        SingleOutputStreamOperator<JSONObject> jsonObj = FlinkSourceUtil.getOdsLog(env,"Dwd_fact_user_register");

//        TODO  2、过滤出用户注册日志数据
        SingleOutputStreamOperator<JSONObject> process = jsonObj.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (value != null && value.getString("type").equals("register"))
                    out.collect(value);
            }});

//        TODO  3、对数据进行清洗，将脏数据输出到侧道
//        定义脏数据侧道输出标签
        OutputTag<String> Dirty = new OutputTag<String>("BM_Dirty") {};
        SingleOutputStreamOperator<JSONObject> result = process.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {

                    String channel = value.getString("channel");
                    JSONObject data = value.getJSONObject("common");

                    Long provinceId = data.getLong("province_id");
                    Long registerId = data.getLong("register_id");
                    Long userId = data.getLong("user_id");

                    if (
                            userId > 0L &&  registerId > 0L && (provinceId > 0 && provinceId < 35)
                    ) {
                        data.put("channel", channel);
                        out.collect(data);
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

//        TODO  4、进行维度关联
        Long ttl = 2*60L;
//        关联用户维度表
        SingleOutputStreamOperator<JSONObject> result_user = DimAssFunction.assUser(result,ttl);
//        关联省份维度表
        SingleOutputStreamOperator<JSONObject> result_province = DimAssFunction.assProvince(result_user,ttl);
//        转换成Json字符串
        SingleOutputStreamOperator<String> result_ss = result_province.map(jsonObject -> jsonObject.toJSONString());

//        TODO  5、将数据输出到kafka上
        result_ss.sinkTo(FlinkSinkUtil.getKafkaSink("BM_DWD_User_Register"));

//        TODO  6、将脏数据输出到kafka上备用
        result.getSideOutput(Dirty).sinkTo(FlinkSinkUtil.getKafkaSink("BM_Dirty"));

//        TODO  7、启动程序
        env.execute("用户注册事实表");
    }
}
