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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.checkerframework.checker.units.qual.A;
import util.FlinkSQLUtil;
import util.FlinkSinkUtil;
import util.FlinkSourceUtil;

import java.util.concurrent.TimeUnit;

/**
 * @基本功能:   流量域——歌曲播放事实表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-16 23:30:07
 **/

/**数据样本
    {
    "common":
        {
         "play_id": 637510,
         "song_id": 10058,
         "user_id": 10042,
         "channel": "APP",
         "lead_song_id": null,
         "start_ts": 1748511173000
         },
     "channel": "APP",
     "mac_id": "281507349970091579",
     "type": "play",
     "ts": 1748511173000
     }
 **/

public class Dwd_fact_traffic_play extends BaseApp {

    public static void main(String[] args) throws Exception {
//        启动程序
        new Dwd_fact_traffic_play()
                .start(
                    10014,
                    4,
                    "Dwd_fact_traffic_play"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) throws Exception {
//        TODO  1、读取日志数据并转化为jsonObject
        SingleOutputStreamOperator<JSONObject> jsonObj = FlinkSourceUtil.getOdsLog(env, "Dwd_fact_traffic_play");

//        TODO  2、过滤出歌曲播放日志数据
        SingleOutputStreamOperator<JSONObject> process = jsonObj.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (value != null && value.getString("type").equals("play"))
                    out.collect(value);
            }});

//        TODO  3、对数据进行清洗，将脏数据输出到侧道
        OutputTag<String> Dirty = new OutputTag<String>("BM_Dirty") {};

        SingleOutputStreamOperator<JSONObject> result = process.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    String channel = value.getString("channel");
                    JSONObject data = value.getJSONObject("common");

                    Long playId = data.getLong("play_id");
                    Long userId = data.getLong("user_id");
                    Long songId = data.getLong("song_id");
                    Long leadSongId = null;
                    try {
                        leadSongId = data.getLong("lead_song_id");  // 如果是数字，正常解析
                    } catch (Exception e) {
                        leadSongId = null;  // 如果是 null 或非数字字符串，设为 null
                    }

                    if (
                            userId > 0L && songId > 0L && playId > 0L
                    ) {
                        data.put("channel", channel);
                        out.collect(data);
                    } else {
//                        类型数值不符合标准
                        value.put("dirty_type", "1");
                        ctx.output(Dirty, value.toJSONString());
                    }

                } catch (Exception e) {
//                    类型转化错误/缺失
                    value.put("dirty_type", "类型转化错误");
                    ctx.output(Dirty, value.toJSONString());
                }
            }
        });

//        TODO  4、对数据进行维度关联
        long ttl = 20 * 60L;
//        关联当前歌曲维度表
        SingleOutputStreamOperator<JSONObject> result_song = DimAssFunction.assSong(result, ttl);
//        关联当前歌曲的歌手维度表
        SingleOutputStreamOperator<JSONObject> result_singer = DimAssFunction.assSinger(result_song, ttl);
//        关联上一首歌曲的歌曲维度表
        SingleOutputStreamOperator<JSONObject> result_leadSong = DimAssFunction.assLeadSong(result_singer, ttl);
//        关联用户维度表
        SingleOutputStreamOperator<JSONObject> result_end = DimAssFunction.assUser(result_leadSong, ttl);
//        最后转化成JsonString
        SingleOutputStreamOperator<String> result_str = result_end.map(jsonObject -> jsonObject.toJSONString());

//        TODO  5、将数据输出到 Kafka 上
        result_str.sinkTo(FlinkSinkUtil.getKafkaSink("BM_DWD_Traffic_Play"));

//        TODO  6、将脏数据输出到 Kafka上备用
        result.getSideOutput(Dirty).sinkTo(FlinkSinkUtil.getKafkaSink("BM_Dirty"));
        result.print();
//        TODO  7、启动程序
        env.execute("歌曲播放事实表");
    }

}
