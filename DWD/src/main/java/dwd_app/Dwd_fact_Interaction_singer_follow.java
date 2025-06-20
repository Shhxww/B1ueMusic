package dwd_app;

import base.BaseApp;
import com.alibaba.fastjson.JSONObject;
import function.AsyncDimFunction;
import function.DimAssFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.FlinkSinkUtil;
import util.FlinkSourceUtil;

import java.util.concurrent.TimeUnit;

/**
 * @基本功能:   交互域——关注歌手事实表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-17 22:45:42
 **/

/**数据样本
 *  {
 *      "common":
 *      {
 *          "follow_id": 679364,
 *          "singer_id": 1001,
 *          "user_id": 10044,
 *          "c_ts": 1751127248000
 *      },
 *      "channel": "PC",
 *      "mac_id": "885502887076827336",
 *      "type": "singer_follow",
 *      "ts": 1751127248000
 *      }
 */

public class Dwd_fact_Interaction_singer_follow extends BaseApp {

    public static void main(String[] args) throws Exception {
//        执行程序
        new Dwd_fact_Interaction_singer_follow().start(
                10016,
                4,
                "Dwd_fact_Interaction_singer_follow"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv,OutputTag<String> Dirty, OutputTag<String> Late) throws Exception {
//        TODO  1、读取日志数据并转化为jsonobj
        SingleOutputStreamOperator<JSONObject> jsonObj = FlinkSourceUtil.getOdsLog(env,"Dwd_fact_Interaction_singer_follow");
//        TODO  2、过滤出歌曲评论日志数据
        SingleOutputStreamOperator<JSONObject> process = jsonObj.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (value != null && value.getString("type").equals("singer_follow")){
                    out.collect(value);
                }
            }});

//        TODO  3、对数据进行清洗，将脏数据输出到侧道
        SingleOutputStreamOperator<JSONObject> result = process.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {

                    String channel = value.getString("channel");
                    JSONObject data = value.getJSONObject("common");

                    Long follow_id = data.getLong("follow_id");
                    Long userId = data.getLong("user_id");
                    Long singerId = data.getLong("singer_id");
                    if (
                            userId > 0L && singerId > 0L && follow_id > 0L
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

        result.print();

//        TODO  4、进行维度关联
        Long ttl = 2*60L;
//        关联用户维度表
        SingleOutputStreamOperator<JSONObject> result_user = DimAssFunction.assUser(result,ttl);
//        关联歌手维度表
        SingleOutputStreamOperator<JSONObject> result_singer = DimAssFunction.assSinger(result_user,ttl);
//        转换成Json字符串
        SingleOutputStreamOperator<String> result_ss = result_singer.map(jsonObject -> jsonObject.toJSONString());
        result_ss.print();
//        TODO  5、将数据输出到kafka上
        result_ss.sinkTo(FlinkSinkUtil.getKafkaSink("BM_DWD_Interaction_Singer_Follow"));

//        TODO  6、将脏数据输出到kafka上备用
        result.getSideOutput(Dirty).sinkTo(FlinkSinkUtil.getKafkaSink("BM_Dirty"));

//        TODO  7、启动程序
        env.execute("关注歌手事实表");
    }
}
