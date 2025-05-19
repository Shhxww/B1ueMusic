package dwd_app;

import base.BaseApp;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.FlinkSQLUtil;
import util.FlinkSinkUtil;
import util.FlinkSourceUtil;

/**
 * @基本功能:   流量域 ——歌曲搜索事实表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-17 20:17:15
 **/

/**  数据样本
    {
    "common":
    {
        "song_id":10053,
        "user_id":10004,
        "s_ts":1748255753000,
        "search_id":824964
    },
    "channel":"APP",
    "type":"search",
    "mac_id":"138132189207238050",
    "ts":1748255753000
    }
 */

public class Dwd_fact_traffic_search extends BaseApp {

    public static void main(String[] args) throws Exception {
//        执行程序
        new Dwd_fact_traffic_search().start(
                10018,
                4,
                "Dwd_fact_traffic_search"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) throws Exception {
//        TODO  1、读取日志数据并转化为jsonobj
        SingleOutputStreamOperator<JSONObject> jsonObj = env
                .fromSource(FlinkSourceUtil.getkafkaSource("BM_log", "Dwd_fact_traffic_search"), WatermarkStrategy.noWatermarks(), "srarchDS")
                .map(new MapFunction<String, JSONObject>() {
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

//        TODO  2、过滤出搜索日志数据
        SingleOutputStreamOperator<JSONObject> process = jsonObj.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (value != null && value.getString("type").equals("search"))
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

                    Long searchId = data.getLong("search_id");
                    Long userId = data.getLong("user_id");
                    Long songId = data.getLong("song_id");
                    Integer provinceId = data.getInteger("province_id");
                    if (
                            userId > 0L && songId > 0L && searchId > 0L && (provinceId > 0 && provinceId < 35)
                    ) {
                        value.put("channel", channel);
                        out.collect(value.toJSONString());
                    } else {
                        value.put("dirty_type", "1");
                        ctx.output(Dirty, value.toJSONString());
                    }
                } catch (Exception e) {
                    value.put("dirty_type", "2");
                    ctx.output(Dirty, value.toJSONString());
                }
            }
        });

//        TODO  4、将数据输出到kafka上
        result.sinkTo(FlinkSinkUtil.getKafkaSink("BM_DWD_Traffic_Search"));

//        TODO  5、将脏数据输出到kafka上备用
        result.getSideOutput(Dirty).sinkTo(FlinkSinkUtil.getKafkaSink("BM_Dirty"));

//        TODO  6、启动程序
        env.execute("歌曲搜索表");
    }
}


