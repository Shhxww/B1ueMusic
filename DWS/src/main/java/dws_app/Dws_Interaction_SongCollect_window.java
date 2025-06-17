package dws_app;

import base.BaseApp;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import function.DorisMapFunction;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * @基本功能:   交互主题——实时——歌曲热度轻度汇总表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-06-03 10:01:55
 **/

/**数据样本
 * {"singer_id":"3001","song_id":10050,"song_duration":"249","collect_id":680120,"user_id":10054,"c_ts":1748793600240,"user_name":"孟非","song_type":"4","channel":"APP","user_gender":"男","song_name":"老人と海"}
 */

public class Dws_Interaction_SongCollect_window extends BaseApp {

    public static void main(String[] args) throws Exception {
        new Dws_Interaction_SongCollect_window().start(
                10024,
                4,
                "Dws_Interaction_SongCollect_window"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv,OutputTag<String> Dirty, OutputTag<String> Late) throws Exception {
//        TODO  1、读取歌曲收藏事实表数据
        DataStreamSource<String> songCollectDS = env.fromSource(
                        FlinkSourceUtil.getkafkaSource("BM_DWD_Interaction_Collect", "Dws_Interaction_SongCollect_window"),
                        WatermarkStrategy.noWatermarks(),
                        "songCollectDS"
                );

//        TODO  2、将数据格式 jsonStr 转换 SongCollectEvent
        SingleOutputStreamOperator<SongCollectEvent> process = songCollectDS.process(new ProcessFunction<String, SongCollectEvent>() {
            @Override
            public void processElement(String value, ProcessFunction<String, SongCollectEvent>.Context ctx, Collector<SongCollectEvent> out) throws Exception {
//                先转化为jsonObject
                JSONObject json = JSONObject.parseObject(value);
//                提取出json中的数据构造成SongCollectEvent
                SongCollectEvent data = SongCollectEvent.builder()
                        .songName(json.getString("song_name"))
                        .songId(json.getLong("song_id"))
                        .songType(json.getString("song_type"))
                        .songDuration(json.getString("song_duration"))
                        .singerId(json.getLong("singer_id"))
                        .collectCount(1L) // 每条记录代表一次收藏操作
                        .updateTime(json.getLong("c_ts"))
                        .build();
//                输出至下游
                out.collect(data);
            }
        });

//        TODO  3、过滤数据并设置水位线(基于事件时间)
        DataStream<SongCollectEvent> eventDS = process
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<SongCollectEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> event.getUpdateTime())
                );

//        TODO   4、按歌曲分组，设置24小时滑动窗口，每5秒更新一次
        OutputTag<SongCollectEvent> bmLateness = new OutputTag<SongCollectEvent>("BM_Lateness"){};
        WindowedStream<SongCollectEvent, Long, TimeWindow> windowsDS = eventDS
                .keyBy(SongCollectEvent::getSongId)
                .window(SlidingEventTimeWindows.of(Time.hours(24), Time.seconds(5)))
                .allowedLateness(Time.seconds(20)) // 允许1分钟延迟数据
                .sideOutputLateData(bmLateness);

//         TODO 5、统计指标
        SingleOutputStreamOperator<SongCollectEvent> result = windowsDS.reduce(
                new ReduceFunction<SongCollectEvent>() {
                    @Override
                    public SongCollectEvent reduce(SongCollectEvent value1, SongCollectEvent value2) throws Exception {
                        return SongCollectEvent
                                .builder()
                                .songId(value1.getSongId())
                                .songName(value1.getSongName())
                                .songType(value1.getSongType())
                                .songDuration(value1.getSongDuration())
                                .singerId(value1.getSingerId())
                                .collectCount(value1.getCollectCount() + value2.getCollectCount())
                                .updateTime(value1.getUpdateTime())
                                .build();
                    }
                },
                new ProcessWindowFunction<SongCollectEvent, SongCollectEvent, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, ProcessWindowFunction<SongCollectEvent, SongCollectEvent, Long, TimeWindow>.Context context, Iterable<SongCollectEvent> elements, Collector<SongCollectEvent> out) throws Exception {

                        SongCollectEvent sce = elements.iterator().next();

                        sce.setPartitionDate(DateFormatUtil.tsToDate(context.window().getEnd()));

                        out.collect(sce);

                    }
                }
        );
//        Dws_Interaction_SongCollect_window.SongCollectEvent(songName=老人と海, songId=10050, singerId=3001, songDuration=249, songType=4, collectCount=50641, windowStart=2025-06-01 00:49:55, windowEnd=2025-06-02 00:49:55, updateTime=1748793600240, partitionDate=2025-06-02)

//        TODO   6、将结果转化jsonStr在写入到 Doris中去
        result.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink("B1ueMusic.Dws_Interaction_SongCollect_window"));

//        TODO  7、启动程序
        env.execute("Dws_Interaction_SongCollect_window");
        }



    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class SongCollectEvent{
        private String songName;
        private Long songId;
        private Long singerId;
        private String songDuration;
        private String songType;
        private Long collectCount;
        @JSONField(serialize = false)  // 单纯拿来推进水位线，在序列化转换成jsonStr时不包含进去
        private Long updateTime;
        private String partitionDate;
    }


}
