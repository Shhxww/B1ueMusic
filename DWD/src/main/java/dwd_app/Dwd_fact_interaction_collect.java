package dwd_app;

import base.BaseApp;
import com.alibaba.fastjson.JSONObject;
import function.AsyncDimFunction;
import function.DimAssFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.FlinkDirtyDateUtil;
import util.FlinkSinkUtil;
import util.FlinkSourceUtil;

import java.util.concurrent.TimeUnit;

import static com.sun.xml.internal.bind.util.Which.which;

/**
 * @基本功能:   互动域——歌曲收藏事实表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-17 15:23:28
 **/

/** 数据样本
    {
    "common":
        {
        "collect_id": 941937,
        "song_id": 10065,
        "user_id": 10031,
        "c_ts": 1749021494000
        },
    "channel": "PC",
    "mac_id": "817233569072956985",
    "type": "collect",
    "ts": 1749021494000
    }
 */

public class Dwd_fact_interaction_collect extends BaseApp {

    public static void main(String[] args) throws Exception {
//        执行程序
        new Dwd_fact_interaction_collect().start(
                10015,
                4,
                "Dwd_fact_interaction_collect"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv,OutputTag<String> Dirty, OutputTag<String> Late) throws Exception {
//        TODO  1、读取日志数据、并转化为jsonobj类型
        DataStreamSource<String> collectDS = env.fromSource(FlinkSourceUtil.getkafkaSource("BM_log", "Dwd_fact_interaction_collect"), WatermarkStrategy.noWatermarks(), "collectDS");
        SingleOutputStreamOperator<JSONObject> jsonObj = collectDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    return jsonObject;
                } catch (Exception e) {
                    return null;
                }
            }
        });

//        TODO  2、过滤出歌曲收藏日志数据
        SingleOutputStreamOperator<JSONObject> cleanDS = jsonObj.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonobj, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (jsonobj != null && jsonobj.getString("type").equals("collect")) {
                        out.collect(jsonobj);
                }
            }
        });

//        TODO  3、将数据进行清洗，将脏数据输出到侧道流

        SingleOutputStreamOperator<JSONObject> result = cleanDS.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    String channel = value.getString("channel");
                    JSONObject data =value.getJSONObject("common");
                    Long userId = data.getLong("user_id");
                    Long songId = data.getLong("song_id");
                    Long collectId = data.getLong("collect_id");
                    if (userId > 0L && songId > 0L && collectId > 0L) {
                        data.put("channel", channel);
                        out.collect(data);
                    } else {
                        ctx.output(Dirty, FlinkDirtyDateUtil.Type1(value));
                    }
                }catch (Exception e){
                    ctx.output(Dirty, FlinkDirtyDateUtil.Type2(value));
                }
            }
        });

//        TODO  4、进行维度关联
        Long ttl = 2*60L;
//        关联用户维度表
        SingleOutputStreamOperator<JSONObject> result_user = DimAssFunction.assUser(result,ttl);
//        关联歌曲维度表
        SingleOutputStreamOperator<JSONObject> result_song = DimAssFunction.assSong(result_user,ttl);
//        转换成Json字符串
        SingleOutputStreamOperator<String> result_ss = result_song.map(jsonObject -> jsonObject.toJSONString());

//        TODO  5、将数据输出到kafka上
        result_ss.sinkTo(FlinkSinkUtil.getKafkaSink("BM_DWD_Interaction_Collect"));

//        TODO  6、将侧道流的脏数据输出到kafka上备用
        result.getSideOutput(Dirty).sinkTo(FlinkSinkUtil.getKafkaSink("BM_Dirty"));

//        TODO  7、启动程序
        env.execute("歌曲收藏事实表");
    }
}
