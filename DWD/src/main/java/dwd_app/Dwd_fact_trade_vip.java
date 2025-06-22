package dwd_app;

import base.BaseApp;
import com.alibaba.fastjson.JSONObject;
import function.AsyncDimFunction;
import function.DimAssFunction;
import function.DorisMapFunction;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.*;

import java.util.concurrent.TimeUnit;

import static util.FlinkSQLUtil.getHiveSink;

/**
 * @基本功能:   交易域——会员开通事实表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-16 23:07:49
 **/

/**数据样本
 * {
 *  "op":"r",
 *  "after":{
 *                  "amount":288,
 *                  "user_id":10052,
 *                  "vip_type":"年",
 *                  "status_type_id":"101",
 *                  "create_ts":1705708800,
 *                  "pay_type":"支付宝",
 *                  "order_num":1,
 *                  "order_id":20035,
 *                  "channel_id":1
 *                  },
 *  "source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"b1uemusic","table":"ods_vip_order"},
 *  "ts_ms":1748937061441
 * }
 */

public class Dwd_fact_trade_vip extends BaseApp {

    public static void main(String[] args) throws Exception {
//        启动程序
        new Dwd_fact_trade_vip().start(
                10014,
                4,
                "Dwd_fact_trade_vip"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv,OutputTag<String> Dirty, OutputTag<String> Late) throws Exception {
//        TODO  1、读取mysql上的开通会员业务数据，并转换为JsonObject类型
        SingleOutputStreamOperator<JSONObject> vipDS = env
                .fromSource(
                        FlinkSourceUtil.getMySqlSource("b1uemusic", "ods_vip_order"),
                        WatermarkStrategy.noWatermarks(),
                        "vipDS")
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

//        TODO  2、将数据进行清洗，将脏数据输出到侧道流
        SingleOutputStreamOperator<JSONObject> process = vipDS.process(new ProcessFunction<JSONObject, JSONObject>() {
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
//                        类型数值不符合标准
                        ctx.output(Dirty, FlinkDirtyDateUtil.Type1(data));
                    }

                } catch (Exception e) {
//                    类型转化错误
                    ctx.output(Dirty, FlinkDirtyDateUtil.Type2(data));
                }
            }
        });

//        TODO  3、进行维度关联
        Long ttl = 2*60L;
        SingleOutputStreamOperator<JSONObject> assUserserDS = AsyncDataStream.unorderedWait(
                process,
                new AsyncDimFunction() {
                    @Override
                    public String getRowKey(JSONObject input) {
                        return "user_id" + input.getLong("user_id").toString();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_user";
                    }

                    @Override
                    public Long getRedisTTL() {
                        return 20*60L;
                    }

                    @Override
                    public void addDims(JSONObject input, JSONObject dim) {
                        String userName = dim.getString("user_name");
                        String gender = dim.getString("gender");
                        String vipEnd = dim.getString("vip_end");
                        if (vipEnd!=null){
                            long ts = Long.parseLong(vipEnd)/1000L;
                            input.put("vip_end",ts);
                        }else {
                            input.put("vip_end",0L);
                        }
                        input.put("user_name", userName);
                        input.put("user_gender", gender);

                    }
                },
                120,
                TimeUnit.SECONDS
        );

//        TODO  4、转换成 POJO
        SingleOutputStreamOperator<vipOrder> result = assUserserDS.map(new MapFunction<JSONObject, vipOrder>() {
            @Override
            public vipOrder map(JSONObject value) throws Exception {
                try {
                    vipOrder vo = JSONObject.parseObject(value.toJSONString(), vipOrder.class);
                    return vo;
                } catch (Exception e) {
                    System.out.println("失败");
                    return null;
                }

            }
        });

//        TODO  5、配置HiveCatalog
        HiveCatalog hiveCatalog = HiveUtil.hiveCatalog;
        tEnv.registerCatalog("myHive", hiveCatalog);

//        TODO  6、先使用HiveCatalog，在将POJO流转化成动态表，
        tEnv.useCatalog("myHive");
        tEnv.createTemporaryView("temp_vip_order",tEnv.fromDataStream(result));

//        TODO  7、将数据输出至hive
        tEnv.executeSql(
                "INSERT INTO b1uemusic.dwd_fact_trade_vip " +
                "SELECT " +
                "  userId as user_id, " +
                "  userName as user_name, " +
                "  vip_end," +
                "  orderId as order_id, " +
                "  orderNum as order_num, " +
                "  amount, " +
                "  statusTypeId as status_type_id, " +
                "  payType as pay_type, " +
                "  vipType as vip_type, " +
                "  userGender as user_gender, " +
                "  CAST(channelId AS BIGINT) as channel_id, " +
                "  create_ts, " +
                "  DATE_FORMAT(FROM_UNIXTIME(create_ts), 'yyyyMMdd') AS dt " +  // 动态分区字段
                "FROM temp_vip_order " +
                "WHERE userId IS NOT NULL AND orderId IS NOT NULL"
        );

//        TODO  8、执行程序
        env.execute("Dwd_fact_trade_vip");
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class vipOrder{
        private Long userId;
        private String userName;
        private Long vip_end;
        private Long orderId;
        private Long orderNum;
        private Long amount;
        private String statusTypeId;
        private String payType;
        private String vipType;
        private String userGender;
        private String channelId;
        private Long create_ts;
    }

}
