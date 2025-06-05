package dws_app;

import base.BaseApp;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import function.DorisMapFunction;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
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
 * @基本功能:   交互主题——实时——歌手热度轻度汇总表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-06-01 22:43:37
 **/

/**数据样本
 *  {"singer_id":3001,"singer_gender":"团体","singer_nationality":"日","user_id":10043,"c_ts":1748793629160,"user_name":"卢强","singer_name":"夜鹿(yorushika)","channel":"APP","user_gender":"男","follow_id":978528}
 */

public class Dws_Interaction_SingerPopularity extends BaseApp {

    public static void main(String[] args) throws Exception {
        new Dws_Interaction_SingerPopularity().start(
                10023,
                4,
                "Dws_Interaction_SingerPopularity"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) throws Exception {
//        TODO  1、读取关注歌手事实表数据
        DataStreamSource<String> singerFollowDS = env.fromSource(
                        FlinkSourceUtil.getkafkaSource("BM_DWD_Interaction_Singer_Follow", "Dws_Interaction_SingerPopularity"),
                        WatermarkStrategy.noWatermarks(),
                        "SingerFollowDS"
                );

//        TODO  2、将数据格式 jsonStr 转换 SingerFollowEvent
        SingleOutputStreamOperator<SingerFollowEvent> process = singerFollowDS.process(new ProcessFunction<String, SingerFollowEvent>() {
            @Override
            public void processElement(String value, ProcessFunction<String, SingerFollowEvent>.Context ctx, Collector<SingerFollowEvent> out) throws Exception {
//                先转化为jsonObject
                JSONObject json = JSONObject.parseObject(value);
//                提取出json中的数据构造成SingerFollowEvent
                SingerFollowEvent data = SingerFollowEvent.builder()
                        .singerName(json.getString("singer_name"))
                        .singerId(json.getLong("singer_id"))
                        .singerNationality(json.getString("singer_nationality"))
                        .singerGender(json.getString("singer_gender"))
                        .followCount(1L) // 每条记录代表一次关注
                        .updateTime(json.getLong("c_ts"))
                        .build();
//                输出至下游
                out.collect(data);
            }
        });

//        TODO  3、过滤数据并设置水位线(基于事件时间)
        DataStream<SingerFollowEvent> eventDS = process
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<SingerFollowEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> event.getUpdateTime())
                );

//        TODO   4、按歌手ID分组，设置24小时滑动窗口，每5秒更新一次
        OutputTag<SingerFollowEvent> bmLateness = new OutputTag<SingerFollowEvent>("BM_Lateness"){};
        WindowedStream<SingerFollowEvent, Long, TimeWindow> windowsDS = eventDS
                .keyBy(SingerFollowEvent::getSingerId)
                .window(SlidingEventTimeWindows.of(Time.hours(24), Time.seconds(5)))
                .allowedLateness(Time.seconds(20)) // 允许1分钟延迟数据
                .sideOutputLateData(bmLateness);

//         TODO 5、统计指标
        SingleOutputStreamOperator<SingerFollowEvent> result = windowsDS.reduce(
                new ReduceFunction<SingerFollowEvent>() {
                    @Override
                    public SingerFollowEvent reduce(SingerFollowEvent value1, SingerFollowEvent value2) throws Exception {
                        value1.setFollowCount(value1.getFollowCount() + value2.getFollowCount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<SingerFollowEvent, SingerFollowEvent, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, ProcessWindowFunction<SingerFollowEvent, SingerFollowEvent, Long, TimeWindow>.Context context, Iterable<SingerFollowEvent> elements, Collector<SingerFollowEvent> out) throws Exception {
                        SingerFollowEvent sfe = elements.iterator().next();

                        sfe.setWindowStart(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        sfe.setWindowEnd(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                        sfe.setPartitionDate(DateFormatUtil.tsToDate(context.window().getEnd()));

                        out.collect(sfe);

                    }
                }
        );
//        Dws_Interaction_SingerPopularity.SingerFollowEvent(singerName=夜鹿(yorushika), singerId=3001, singerNationality=日, singerGender=团体, followCount=17223, windowStart=2025-06-01 00:01:00, windowEnd=2025-06-02 00:01:00, updateTime=1748793659160, partitionDate=2025-06-02)


//        TODO   6、将结果转化jsonStr在写入到 Doris中去
        result.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(""));

//        TODO  7、启动程序
        env.execute("Dws_Interaction_SingerPopularity");
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class SingerFollowEvent{
        private String singerName;
        private Long singerId;
        private String singerNationality;
        private String singerGender;
        private Long followCount;
        private String windowStart;
        private String windowEnd;
        @JSONField(serialize = false)  // 单纯拿来推进水位线，在序列化转换成jsonStr时不包含进去
        private Long updateTime;
        private String partitionDate;
    }

}
