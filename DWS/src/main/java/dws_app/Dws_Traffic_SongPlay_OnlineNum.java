package dws_app;

import base.BaseApp;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import function.DorisMapFunction;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.DateFormatUtil;
import util.FlinkSQLUtil;
import util.FlinkSinkUtil;
import util.FlinkSourceUtil;

import java.time.Duration;
import java.util.Map;
import java.util.Random;

/**
 * @基本功能:   流量主题——实时——歌曲在线播放人数轻度汇总表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-24 11:48:13
 **/

/**数据样本
    {
        "start_ts":1748275250760,
         "singer_gender":"女",
         "singer_nationality":"中",
         "lead_song_id":10046,
         "song_duration":"242",
         "song_type":"3",
         "singer_name":"邓紫棋",
         "user_name":"田雨",
         "channel":"APP",
         "song_name":"喜欢你",
         "lead_song_type":"1",
         "user_gender":"女",
         "lead_singer_id":"3001",
         "lead_song_duration":"241",
         "singer_id":"1003",
         "song_id":10015,
         "play_id":321100,
         "user_id":10032,
         "lead_song_name":"だから僕は音楽を辞めた"
    }
 */

public class Dws_Traffic_SongPlay_OnlineNum extends BaseApp {

    public static void main(String[] args) throws Exception {
        new Dws_Traffic_SongPlay_OnlineNum().start(
                        10021,
                        4,
                        "Dws_Traffic_SongPlay_OnlineNum");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, OutputTag<String> Dirty, OutputTag<String> Late) throws Exception {

//        TODO  1、读取歌曲播放事实表数据
        DataStreamSource<String> songPlayDS = env
                .fromSource(
                        FlinkSourceUtil.getkafkaSource("BM_DWD_Traffic_Play", "Dws_Traffic_SongPlay_OnlineNum"),
                        WatermarkStrategy.noWatermarks(),
                        "SongPlayDS"
                );

//        TODO  2、将数据转化为 jsonObject，并将数据切分成开始播放和结束播放俩种
        SingleOutputStreamOperator<SongChangeEvent> process = songPlayDS.process(
                new ProcessFunction<String, SongChangeEvent>( ) {
            @Override
            public void processElement(String jsonstr, ProcessFunction<String, SongChangeEvent>.Context ctx, Collector<SongChangeEvent> out) throws Exception {

                JSONObject jsonObject = JSONObject.parseObject(jsonstr);

                Long ts = jsonObject.getLong("start_ts");

                SongChangeEvent enter = SongChangeEvent.builder()
                        .songId(jsonObject.getLong("song_id"))
                        .songName(jsonObject.getString("song_name"))
                        .singerName(jsonObject.getString("singer_name"))
                        .userId(jsonObject.getLong("user_id"))
                        .userName(jsonObject.getString("user_name"))
                        .changeType("enter")
                        .ts(ts)
                        .build();
                out.collect(enter);

//                用户的第一条数据没有lead_song
                Long leadSongId = jsonObject.getLong("lead_song_id");
                if ( leadSongId!=null ) {
                    SongChangeEvent leave = SongChangeEvent
                            .builder()
                            .songId(leadSongId)
                            .songName(jsonObject.getString("lead_song_name"))
                            .singerName(jsonObject.getString("lead_singer_name"))
                            .userId(jsonObject.getLong("user_id"))
                            .userName(jsonObject.getString("user_name"))
                            .changeType("leave")
                            .ts(ts)
                            .build();
                    out.collect(leave);
                }
            }
        });

//        TODO  3、按照歌曲id进行分组
        KeyedStream<SongChangeEvent, Long> songChangeEventLongKeyedStream = process.keyBy(s -> s.getSongId());

//        TODO  4、统计歌曲在线收听人数，定时器每 5s输出一次
        SingleOutputStreamOperator<SongOnlineStatus> processed = songChangeEventLongKeyedStream.process(
                new KeyedProcessFunction<Long, SongChangeEvent, SongOnlineStatus>() {

                    private MapState<Long, String> onlineUsers;

                    private ValueState<Long> ts;

                    private ValueState<SongOnlineStatus> sos;

                    @Override
                    public void open(Configuration parameters) throws Exception {
//                        初始化状态
                        onlineUsers = getRuntimeContext().getMapState(new MapStateDescriptor<>("OnlineUsers", Long.class, String.class));
                        sos = getRuntimeContext().getState(new ValueStateDescriptor<>("OnlineStatus", SongOnlineStatus.class));
                        ts = getRuntimeContext().getState(new ValueStateDescriptor<>("Ts", Long.class));
                    }

                    @Override
                    public void processElement(SongChangeEvent sc, KeyedProcessFunction<Long, SongChangeEvent, SongOnlineStatus>.Context ctx, Collector<SongOnlineStatus> out) throws Exception {

//                        获取切换类型，并增删收听用户
                        String changeType = sc.getChangeType();
                        if (changeType.equals("enter")) {
                            onlineUsers.put(sc.userId, sc.userName);
                        } else if (changeType.equals("leave")) {
                            onlineUsers.remove(sc.userId);
                        }

//                         初始化歌曲基本信息
                        if (sos.value() == null) {
                            SongOnlineStatus songOnlineStatus = SongOnlineStatus
                                    .builder()
                                    .songId(sc.getSongId())
                                    .songName(sc.getSongName())
                                    .singerName(sc.getSingerName())
                                    .build();
                            sos.update(songOnlineStatus);
                        }

//                        设置第一个定时器，每5秒输出一次结果
                        if (ts.value() == null) {
                            long timerTs = ((ctx.timestamp() / 5000) + 1) * 5000; // 对齐到5秒边界
                            ctx.timerService().registerProcessingTimeTimer(timerTs);
                            ts.update(timerTs);
                        }
                    }

                    /**
                     * 统计当前在线收听人数，并将数据传入下游
                     */
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Long, SongChangeEvent, SongOnlineStatus>.OnTimerContext ctx, Collector<SongOnlineStatus> out) throws Exception {
//                        统计当前在线收听人数
                        Long currentOnlineCount = 0L;
                        if (onlineUsers.iterator().hasNext()) {
                            for (Map.Entry<Long, String> entry : onlineUsers.entries()) {
                                currentOnlineCount++;
                            }
                        }

//                        更新状态，并输出至下游
                        SongOnlineStatus info = sos.value();
                        if (info != null && currentOnlineCount >= 0) {
                            info.setCurrentOnlineCount(currentOnlineCount);
                            info.setPartitionTime(DateFormatUtil.tsToDate(timestamp));
                            sos.update(info);
                            out.collect(info);
                        }

                        // 设置下一个定时器
                        long nextTimer = timestamp + 5000; // 下一个5秒
                        ctx.timerService().registerProcessingTimeTimer(nextTimer);
                        ts.update(nextTimer);
                    }
                });


//        TODO  5、转换成 jsonstr
        SingleOutputStreamOperator<String> result = processed.map(new DorisMapFunction<>());

//        TODO  6、输出至 Doris
        result.sinkTo(FlinkSinkUtil.getDorisSink("B1ueMusic.Dws_Traffic_SongPlay_OnlineNum"));

//        TODO  7、执行程序
        env.execute("歌曲在线人数");
    }

    /**
     *歌曲切换事件
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class SongChangeEvent{
        public Long songId;
        public String songName;
        public String singerName;
        public Long userId;
        public String userName;
        public String changeType;  // ENTER 或 LEAVE
        public Long currentOnlineCount;
        public long ts;
    }

    /**
     * 歌曲在线状态
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class SongOnlineStatus {
        private Long songId;
        private String songName;
        private String singerName;
        private Long currentOnlineCount;
        private String partitionTime;
    }


}
