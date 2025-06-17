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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
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
import java.util.HashSet;
import java.util.Set;

/**
 * @基本功能:   流量主题——实时——各省份用户活跃度汇总表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-06-17 15:30:00
 **/

/**
 * 数据样本
 * {"login_id":526692,"user_id":10039,"province_id":27,"login_ts":1749571200240,"user_name":"薛凯","channel":"PC","user_gender":"男","province_name":"陕西省"}
 */

public class Dws_Traffic_ProvinceUserActivity_window extends BaseApp {

    public static void main(String[] args) throws Exception {
        new Dws_Traffic_ProvinceUserActivity_window().start(
                10025,
                4,
                "Dws_Traffic_ProvinceUserActivity_window"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, OutputTag<String> Dirty, OutputTag<String> Late) throws Exception {
//        TODO  1、读取用户登录事实表数据
        DataStreamSource<String> userLoginDS = env
                .fromSource(
                        FlinkSourceUtil.getkafkaSource("BM_DWD_User_Login", "Dws_Traffic_ProvinceUserActivity_window"),
                        WatermarkStrategy.noWatermarks(),
                        "UserLoginDS"
                );

//        TODO  2、转换成省份用户活跃度统计类型
        SingleOutputStreamOperator<ProvinceUserActivityEvent> process = userLoginDS.process(
                new ProcessFunction<String, ProvinceUserActivityEvent>() {
                @Override
                public void processElement(String value, ProcessFunction<String, ProvinceUserActivityEvent>.Context ctx, Collector<ProvinceUserActivityEvent> out) throws Exception {
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(value);

                        // 创建用户ID集合用于去重统计
                        Set<Long> userIdSet = new HashSet<>();
                        userIdSet.add(jsonObject.getLong("user_id"));

                        ProvinceUserActivityEvent puae = ProvinceUserActivityEvent
                                .builder()
                                .provinceId(jsonObject.getLong("province_id"))
                                .provinceName(jsonObject.getString("province_name"))
                                .activeUserIds(userIdSet)  // 用于计算活跃用户数
                                .updateTime(jsonObject.getLong("login_ts"))
                                .build();
                        out.collect(puae);

                    } catch (Exception e) {

                        System.out.println("转换失败");
                    }
                }
        });

//        TODO  3、设置水位线
        SingleOutputStreamOperator<ProvinceUserActivityEvent> puaeDS = process.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<ProvinceUserActivityEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> event.getUpdateTime())
        );

//        TODO  4、按照省份id进行分组,开窗
        WindowedStream<ProvinceUserActivityEvent, Long, TimeWindow> window = puaeDS
                .keyBy(puae -> puae.getProvinceId())
                .window(SlidingEventTimeWindows.of(Time.hours(1L), Time.seconds(5))); // 1小时窗口，每10秒滑动一次

//        TODO  5、进行聚合，统计在1小时内各省份的用户活跃度
        SingleOutputStreamOperator<ProvinceUserActivityEvent> result = window.reduce(
                new ReduceFunction<ProvinceUserActivityEvent>() {
                    @Override
                    public ProvinceUserActivityEvent reduce(ProvinceUserActivityEvent value1, ProvinceUserActivityEvent value2) throws Exception {
                        // 合并用户ID集合用于去重
                        Set<Long> mergedUserIds = new HashSet<>(value1.getActiveUserIds());
                        mergedUserIds.addAll(value2.getActiveUserIds());

                        return ProvinceUserActivityEvent
                                .builder()
                                .provinceId(value1.getProvinceId())
                                .provinceName(value1.getProvinceName())
                                .activeUserIds(mergedUserIds)
                                .build();
                    }
                },
                new ProcessWindowFunction<ProvinceUserActivityEvent, ProvinceUserActivityEvent, Long, TimeWindow>() {
                    @Override
                    public void process(Long provinceId, ProcessWindowFunction<ProvinceUserActivityEvent, ProvinceUserActivityEvent, Long, TimeWindow>.Context context, Iterable<ProvinceUserActivityEvent> elements, Collector<ProvinceUserActivityEvent> out) throws Exception {

                        ProvinceUserActivityEvent puae = elements.iterator().next();

                        // 设置活跃用户数（去重后的用户数量）
                        puae.setActiveUserCount((long) puae.getActiveUserIds().size());

                        // 设置窗口结束时间作为分区时间
                        puae.setPartitionDate(DateFormatUtil.tsToDate(context.window().getEnd()));

                        // 计算窗口时间范围
                        puae.setWindowStart(DateFormatUtil.tsToDate(context.window().getStart()));
                        puae.setWindowEnd(DateFormatUtil.tsToDate(context.window().getEnd()));

                        out.collect(puae);
                    }
                }
        );

//        TODO  6、转化输出类型，并输出至Doris
//        3> Dws_Traffic_ProvinceUserActivity_window.ProvinceUserActivityEvent(provinceId=27, provinceName=陕西省, activeUserCount=1, activeUserIds=[10039], updateTime=null, partitionDate=2025-06-17, windowStart=2025-06-17, windowEnd=2025-06-17)
        result
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("B1ueMusic.Dws_Traffic_ProvinceUserActivity_window"));

//        TODO  7、执行程序
        env.execute("Dws_Traffic_ProvinceUserActivity_window");
    }

    /**
     * 省份用户活跃度事件
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class ProvinceUserActivityEvent{
        private Long provinceId;           // 省份ID
        private String provinceName;       // 省份名称
        private Long activeUserCount;      // 活跃用户数（去重）
        @JSONField(serialize = false)
        private Set<Long> activeUserIds;   // 活跃用户ID集合（用于去重计算，不序列化）
        @JSONField(serialize = false)
        private Long updateTime;           // 更新时间（用于水位线推进，不序列化）
        private String partitionDate;      // 分区时间
        private String windowStart;        // 窗口开始时间
        private String windowEnd;          // 窗口结束时间
    }
}