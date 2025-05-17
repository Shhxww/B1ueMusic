package dwd_app;

import base.BaseApp;
import bean.Dwd_collect;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.FlinkSinkUtil;
import util.FlinkSourceUtil;

import static com.sun.xml.internal.bind.util.Which.which;

/**
 * @基本功能:   歌曲收藏事实表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-17 15:23:28
 **/

public class Dwd_fact_interaction_collect extends BaseApp {

    public static void main(String[] args) throws Exception {
//        执行程序
        new Dwd_fact_interaction_collect().start(
                10016,
                4,
                "Dwd_fact_interaction_collect"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) throws Exception {
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
                if (jsonobj != null) {
                    if (jsonobj.get("type") == "collect") {
                        out.collect(jsonobj);
                    }
                }
            }
        });

//        TODO  3、将数据进行清洗，将脏数据输出到侧道流，转化为事实表数据类型
        OutputTag<String> collectDirty = new OutputTag<String>("collect_dirty");
        SingleOutputStreamOperator<String> result = cleanDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                String channel = value.getString("channel");
                JSONObject data =value.getJSONObject("common");
                if (data.getLong("uesr_id") > 0L && data.getLong("song_id") > 0L && data.getLong("collect_id") > 0) {
                    data.put("channel", channel);
                    out.collect(data.toJSONString());
                } else {
                    value.put("dirty_type", "1");
                    ctx.output(collectDirty, value.toJSONString());
                }
            }
        });

//        TODO  4、将数据输出到kafka上
        result.sinkTo(FlinkSinkUtil.getKafkaSink("BM_DWD_Interaction_Collect"));

//        TODO  5、将侧道流的脏数据输出到kafka上备用
        result.getSideOutput(collectDirty).sinkTo(FlinkSinkUtil.getKafkaSink("BM_Drity"));

//        TODO  6、启动程序
        env.execute("");

    }
}
