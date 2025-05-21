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
 * @基本功能:   互动域——歌曲评论事实表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-17 22:24:00
 **/

/**数据样本
    {
     "common":
        {
            "comment_id": 758026,
             "song_id": 10021,
             "user_id": 10014,
             "content": "太棒了!",
             "province_id": 21,
             "comment_date": 1750375869000
             },
     "channel": "PC",
     "mac_id": "937906742620298677",
     "type": "comment",
     "ts": 1750375869000
     }
 */

public class Dwd_fact_interaction_comment extends BaseApp {

    public static void main(String[] args) throws Exception {
//        执行程序
        new Dwd_fact_interaction_comment().start(
                10017,
                4,
                "Dwd_fact_interaction_comment"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) throws Exception {
//        TODO  1、读取日志数据并转化为jsonobj
        SingleOutputStreamOperator<JSONObject> jsonObj = env
                .fromSource(FlinkSourceUtil.getkafkaSource("BM_log", "Dwd_fact_interaction_comment"), WatermarkStrategy.noWatermarks(), "srarchDS")
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

//        TODO  2、过滤出歌曲评论日志数据
        SingleOutputStreamOperator<JSONObject> process = jsonObj.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (value != null && value.getString("type").equals("comment"))
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

                    Long comment_id = data.getLong("comment_id");
                    Long userId = data.getLong("user_id");
                    Long songId = data.getLong("song_id");
                    Integer provinceId = data.getInteger("province_id");
                    if (
                            userId > 0L && songId > 0L && comment_id > 0L && (provinceId > 0 && provinceId < 35)
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
//        关联歌曲维度表
        SingleOutputStreamOperator<JSONObject> result_song = DimAssFunction.assSong(result_province,ttl);
//        转换成Json字符串
        SingleOutputStreamOperator<String> result_ss = result_song.map(jsonObject -> jsonObject.toJSONString());

//        TODO  5、将数据输出到kafka上
        result_ss.sinkTo(FlinkSinkUtil.getKafkaSink("BM_DWD_Interaction_Comment"));
        result_ss.print();
//        TODO  6、将脏数据输出到kafka上备用
        result.getSideOutput(Dirty).sinkTo(FlinkSinkUtil.getKafkaSink("BM_Dirty"));
//      {"song_duration":"204","user_name":"江珊","song_type":"3","channel":"APP","comment_date":1748793600600,"user_gender":"女","song_name":"那XX(That XX)","comment_id":853261,"content":"好难听","province_name":"天津市","singer_id":"2001","song_id":10028,"user_id":10057,"province_id":2}
//        TODO  7、启动程序
        env.execute("歌曲评论事实表");
    }
}
