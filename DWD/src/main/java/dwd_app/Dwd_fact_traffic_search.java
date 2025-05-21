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
import util.FlinkSQLUtil;
import util.FlinkSinkUtil;
import util.FlinkSourceUtil;

import java.util.concurrent.TimeUnit;

/**
 * @基本功能:   流量域 ——歌曲搜索事实表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-17 20:17:15
 **/

/**  数据样本
    {
     "common": {
         "search_id": 320488,
         "song_id": 10036,
         "user_id": 10019,
         "province_id": 14,
         "s_ts": 1748147260000
         },
     "channel": "APP",
     "mac_id": "623321518290402901",
     "type": "search",
     "ts": 1748147260000
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
//        TODO  1、读取日志数据并转化为 jsonObject，并过滤出查询日志数据
        SingleOutputStreamOperator<JSONObject> search = FlinkSourceUtil.getOdsLog(env, "Dwd_fact_traffic_search");

//        TODO  2、过滤出搜索日志数据
        SingleOutputStreamOperator<JSONObject> jsonObj = search.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (value.getString("type").equals("search")) {
                    out.collect(value);
                }
            }
        });


//        TODO  3、对数据进行清洗，将脏数据输出到侧道
//        定义脏数据侧道输出标签
        OutputTag<String> Dirty = new OutputTag<String>("BM_Dirty") {};
        SingleOutputStreamOperator<JSONObject> result = jsonObj.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
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
                        data.put("channel", channel);
                        out.collect(data);
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

//        TODO  4、进行维度关联
        Long ttl =2*60L;
//        关联用户维度表
        SingleOutputStreamOperator<JSONObject> result_user = DimAssFunction.assUser(result,ttl);
//        关联省份维度表
        SingleOutputStreamOperator<JSONObject> result_province = DimAssFunction.assProvince(result_user,ttl);
//        关联歌曲维度表
        SingleOutputStreamOperator<JSONObject> result_song = DimAssFunction.assSong(result_province,ttl);
//        转换成Json字符串
        SingleOutputStreamOperator<String> result_ss = result_song.map(jsonObject -> jsonObject.toJSONString());

//        TODO  5、将数据输出到kafka上
        result_ss.sinkTo(FlinkSinkUtil.getKafkaSink("BM_DWD_Traffic_Search"));

//        TODO  6、将脏数据输出到kafka上备用
        result.getSideOutput(Dirty).sinkTo(FlinkSinkUtil.getKafkaSink("BM_Dirty"));

//        TODO  7、启动程序
        env.execute("歌曲搜索表");
    }
}


