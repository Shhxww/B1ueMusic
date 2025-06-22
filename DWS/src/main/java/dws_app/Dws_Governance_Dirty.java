package dws_app;

import base.BaseApp;
import com.alibaba.fastjson.JSONObject;
import com.sun.xml.internal.bind.v2.TODO;
import function.DorisMapFunction;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
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
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.DateFormatUtil;
import util.FlinkSinkUtil;
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

//        TODO  1、获取脏数据流并转化为 dirtyCountStatus
        SingleOutputStreamOperator<dirtyCountStatus> dirtyDS = env
                .fromSource(
                        FlinkSourceUtil.getkafkaSource("BM_Dirty", "Dws_Governance_Dirty"),
                        WatermarkStrategy.noWatermarks(),
                        "dirtyDS").map(new MapFunction<String, dirtyCountStatus>() {
                    @Override
                    public dirtyCountStatus map(String value) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        dirtyCountStatus dirtyCountStatus = Dws_Governance_Dirty.dirtyCountStatus
                                .builder()
                                .dirty_type(jsonObject.getString("dirty_type"))
                                .count(1)
                                .build();
                        return dirtyCountStatus;
                    }
                });

//        TODO  2、按类型进行分类
        KeyedStream<dirtyCountStatus, String> keyedDS = dirtyDS.keyBy( j -> j.dirty_type);

//        TODO  3、开窗
        WindowedStream<dirtyCountStatus, String, TimeWindow> windowDS = keyedDS.window(SlidingEventTimeWindows.of(Time.hours(1L), Time.minutes(1L)));

//        TODO  4、统计
        SingleOutputStreamOperator<dirtyCountStatus> reduce = windowDS.reduce(new ReduceFunction<dirtyCountStatus>() {
            @Override
            public dirtyCountStatus reduce(dirtyCountStatus value1, dirtyCountStatus value2) throws Exception {
                return dirtyCountStatus
                        .builder()
                        .dirty_type(value1.dirty_type)
                        .count(value1.count + value2.count)
                        .build();
            }
        }, new ProcessWindowFunction<dirtyCountStatus, dirtyCountStatus, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<dirtyCountStatus, dirtyCountStatus, String, TimeWindow>.Context context, Iterable<dirtyCountStatus> elements, Collector<dirtyCountStatus> out) throws Exception {
                dirtyCountStatus dcs = elements.iterator().next();
                dcs.setPartitionDate(DateFormatUtil.tsToDate(context.window().getEnd()));

//                计算窗口时间范围
                dcs.setWindowStart(DateFormatUtil.tsToDate(context.window().getStart()));
                dcs.setWindowEnd(DateFormatUtil.tsToDate(context.window().getEnd()));

                out.collect(dcs);
            }
        });

//        TODO  5、输出至Doris
        SingleOutputStreamOperator<String> result = reduce.map(new DorisMapFunction<>());
        result.sinkTo(FlinkSinkUtil.getDorisSink(""));
        result.print();

//        TODO  6、执行程序
        env.execute("Dws_Governance_Dirty");
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class dirtyCountStatus{
        private String dirty_type;
        private Integer count;
        private String windowStart;
        private String windowEnd;
        private String partitionDate;
    }

}
