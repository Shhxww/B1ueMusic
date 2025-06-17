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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
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
 * @基本功能:   流量主题——实时——歌曲搜索轻度汇总表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-31 21:59:31
 **/

/**
 * 数据样本
 * {"song_duration":"270","user_name":"潘虹","song_type":"3","channel":"APP","user_gender":"女","song_name":"江南","search_id":260826,"province_name":"青海省","singer_id":"1002","song_id":10006,"user_id":10030,"province_id":29,"s_ts":1748707259880}
 */

public class Dws_Traffic_SongSearch_window extends BaseApp {

    public static void main(String[] args) throws Exception {
        new Dws_Traffic_SongSearch_window().start(
                10022,
                4,
                "Dws_Traffic_SongSearch_window"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, OutputTag<String> Dirty, OutputTag<String> Late) throws Exception {
//        TODO  1、读取歌曲搜索事实表数据
        DataStreamSource<String> songSearchDS = env
                .fromSource(
                        FlinkSourceUtil.getkafkaSource("BM_DWD_Traffic_Search", "Dws_Traffic_SongSearch_window"),
                        WatermarkStrategy.noWatermarks(),
                        "SongSearchDS"
                );

//        TODO  2、转换成SongSearchEvent
        SingleOutputStreamOperator<SongSearchEvent> process = songSearchDS.process(
                new ProcessFunction<String, SongSearchEvent>() {
                @Override
                public void processElement(String value, ProcessFunction<String, SongSearchEvent>.Context ctx, Collector<SongSearchEvent> out) throws Exception {
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        SongSearchEvent sse = SongSearchEvent
                                .builder()
                                .songId(jsonObject.getLong("song_id"))
                                .songName(jsonObject.getString("song_name"))
                                .singerId(jsonObject.getLong("singer_id"))
                                .singerName(jsonObject.getString("singer_name"))
                                .updateTime(jsonObject.getLong("s_ts"))
                                .searchCount(1L)
                                .build();
                        out.collect(sse);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
        });

//        TODO  3、设置水位线
        SingleOutputStreamOperator<SongSearchEvent> sseDS = process.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<SongSearchEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> event.getUpdateTime())
        );

//        TODO  4、按照歌曲id进行分组,开窗
        WindowedStream<SongSearchEvent, Long, TimeWindow> window = sseDS
                .keyBy(sse -> sse.getSongId())
                .window(SlidingProcessingTimeWindows.of(Time.days(1L), Time.seconds(5)));

//        TODO  5、进行聚合，统计在24小时内每首歌曲的搜索量
        SingleOutputStreamOperator<SongSearchEvent> result = window.reduce(
                new ReduceFunction<SongSearchEvent>() {
                    @Override
                    public SongSearchEvent reduce(SongSearchEvent value1, SongSearchEvent value2) throws Exception {
                        return SongSearchEvent
                                .builder()
                                .songId(value1.getSongId())
                                .songName(value1.getSongName())
                                .singerId(value1.getSingerId())
                                .singerName(value1.getSingerName())
                                .searchCount(value1.getSearchCount() + value2.getSearchCount())
                                .build();
                    }
                },
                new ProcessWindowFunction<SongSearchEvent, SongSearchEvent, Long, TimeWindow>() {
                    @Override
                    public void process(Long songId, ProcessWindowFunction<SongSearchEvent, SongSearchEvent, Long, TimeWindow>.Context context, Iterable<SongSearchEvent> elements, Collector<SongSearchEvent> out) throws Exception {

                        SongSearchEvent sse = elements.iterator().next();

                        sse.setPartitionTime(DateFormatUtil.tsToDate(context.window().getEnd()));

                        out.collect(sse);
                    }
                }
        );

//        TODO  6、转化输出类型，并输出至Doris
        result
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("B1ueMusic.Dws_Traffic_SongSearch_window"));

//        TODO  7、执行程序
        env.execute("Dws_Traffic_SongSearch_window");
    }

    /**
     * 歌曲搜索事件
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class SongSearchEvent{
        private Long songId;
        private String songName;
        private Long singerId;
        private String singerName;
        private Long searchCount;
        @JSONField(serialize = false)  // 单纯拿来推进水位线，在序列化转换成jsonStr时不包含进去
        private Long updateTime;
        private String partitionTime;
    }

}

