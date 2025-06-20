package dws_app;

import base.BaseApp;
import com.alibaba.fastjson.JSONObject;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.OutputTag;
import util.FlinkSourceUtil;

import java.time.Duration;

/**
 * @基本功能:   数据治理——脏数据处理
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-06-17 22:49:17
 **/

public class Dws_Governance_Dirty extends BaseApp {

    public static void main(String[] args) throws Exception {
        new Dws_Governance_Dirty().start(
                10029,
                4,
                "Dws_Governance_Dirty"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, OutputTag<String> Dirty, OutputTag<String> Late) throws Exception {

//        TODO  1、获取脏数据流并转化为 jsonObject
        SingleOutputStreamOperator<JSONObject> dirtyDS = env
                .fromSource(
                        FlinkSourceUtil.getkafkaSource("BM_Dirty", "Dws_Governance_Dirty"),
                        WatermarkStrategy.noWatermarks(),
                        "dirtyDS").map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSONObject.parseObject(value);
                    }
                });

        SingleOutputStreamOperator<JSONObject> logDS = FlinkSourceUtil.getOdsLog(env, "Dws_Governance_Dirty");
//        TODO  2、设置水位线
        SingleOutputStreamOperator<JSONObject> dirtyWM = dirtyDS
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((j,l)->j.getLong("ts"))
                );

//        TODO  3、按来源进行分类
        KeyedStream<JSONObject, String> keyedDS = dirtyWM.keyBy(j -> j.getString("Source"));

//        TODO  4、开窗
        WindowedStream<JSONObject, String, TimeWindow> windowDS = keyedDS.window(SlidingEventTimeWindows.of(Time.hours(1L), Time.minutes(1L)));

//        TODO  5、统计
//        SingleOutputStreamOperator<JSONObject> reduced = windowDS.reduce(
//                new ReduceFunction<JSONObject>() {
//                    @Override
//                    public JSONObject reduce(JSONObject value1, JSONObject value2) throws Exception {
//                        return null;
//                    }
//                },
//                new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
//                }
//
//        );

//        TODO  、
//        TODO  、
//        TODO  、
//        TODO  、执行程序
        env.execute("Dws_Governance_Dirty");
    }
}
