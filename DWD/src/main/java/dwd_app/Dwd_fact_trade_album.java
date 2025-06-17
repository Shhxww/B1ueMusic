package dwd_app;

import base.BaseApp;
import com.alibaba.fastjson.JSONObject;
import function.AsyncDimFunction;
import function.DimAssFunction;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.FlinkSQLUtil;
import util.FlinkSourceUtil;
import util.HiveUtil;

import java.util.concurrent.TimeUnit;

/**
 * @基本功能:   交易域——专辑购买事实表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-19 21:17:34
 **/

public class Dwd_fact_trade_album extends BaseApp {

    public static void main(String[] args) throws Exception {
        new Dwd_fact_trade_album().start(
                10021,
                4,
                "Dwd_fact_trade_album"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv,OutputTag<String> Dirty, OutputTag<String> Late) throws Exception {

//        TODO  1、从MySQL读取专辑购买业务数据，转化为JsonObject
         SingleOutputStreamOperator<JSONObject> albumDS = env
                .fromSource(
                        FlinkSourceUtil.getMySqlSource("b1uemusic", "ods_album_order"),
                        WatermarkStrategy.noWatermarks(),
                        "albumDS")
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject data = JSONObject.parseObject(value).getJSONObject("after");
                            out.collect(data);
                        } catch (Exception e) {
                            System.out.println("1：转化失败");
                        }
                    }
                });

//        TODO  2、筛选出成功的订单业务数据
        SingleOutputStreamOperator<JSONObject> process = albumDS.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject data, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    Long userId = data.getLong("user_id");
                    Long orderId = data.getLong("order_id");
                    Long orderNum = data.getLong("order_num");
                    Long amount = data.getLong("amount");
                    String statusTypeId = data.getString("status_type_id");
                    if (userId > 0L && orderId > 0L && orderNum > 0L && amount > 0L && statusTypeId != null ) {
                        if (statusTypeId.equals("101")) out.collect(data);
                    } else {
                        data.put("dirty_type", "1");
                        ctx.output(Dirty, data.toJSONString());
                    }
                } catch (Exception e) {
                    data.put("dirty_type", "2");
                    ctx.output(Dirty, data.toJSONString());
                }
            }
        });

//        TODO  3、进行维度关联
        SingleOutputStreamOperator<JSONObject> assUser = DimAssFunction.assUser(process,20*60L);

//        TODO  4、转化为POJO
//        {"amount":170,"user_id":10003,"status_type_id":"101","user_name":"李娜","create_ts":1673539200,"album_id":5013,"pay_type":"微信","user_gender":"女","order_num":1,"order_id":30013,"channel_id":1}
        SingleOutputStreamOperator<albumOrder> result = assUser.map(new MapFunction<JSONObject, albumOrder>() {
            @Override
            public albumOrder map(JSONObject value) throws Exception {
                try {
                    return JSONObject.parseObject(value.toJSONString(), albumOrder.class);
                } catch (Exception e) {
                    System.out.println("失败");
                    return null;
                }
            }
        });

//        TODO  5、配置HiveCatalog
        HiveCatalog hiveCatalog = HiveUtil.hiveCatalog;
        tEnv.registerCatalog("myHive", hiveCatalog);

//        TODO  6、先使用HiveCatalog，在将POJO流转化成动态表
        tEnv.useCatalog("myHive");
        tEnv.createTemporaryView("temp_album_order",result);
//        Dwd_fact_trade_album.albumOrder(userId=10053, userName=顾城, userGender=男, orderId=30023, orderNum=1, amount=270, statusTypeId=101, payType=支付宝, album_id=5023, channelId=1, create_ts=1704672000)

//        TODO  7、将数据输出至hive
        tEnv.executeSql(
                "INSERT INTO b1uemusic.dwd_fact_trade_album " +
                "SELECT " +
                "  userId as user_id, " +
                "  userName as user_name, " +
                "  orderId as order_id, " +
                "  orderNum as order_num, " +
                "  amount, " +
                "  statusTypeId as status_type_id, " +
                "  payType as pay_type, " +
                "  album_id as album_id, " +
                "  userGender as user_gender, " +
                "  CAST(channelId AS BIGINT) as channel_id, " +
                "  create_ts, " +
                "  DATE_FORMAT(FROM_UNIXTIME(create_ts), 'yyyyMMdd') AS dt " +  // 动态分区字段
                "FROM temp_album_order " +
                "WHERE userId IS NOT NULL AND orderId IS NOT NULL"
        );

//        TODO  8、启动程序
        env.execute("Dwd_fact_trade_album");
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class albumOrder{
        private Long userId;
        private String userName;
        private String userGender;
        private Long orderId;
        private Long orderNum;
        private Long amount;
        private String statusTypeId;
        private String payType;
        private String album_id;
        private String channelId;
        private Long create_ts;
    }

}
