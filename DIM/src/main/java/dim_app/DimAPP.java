package dim_app;

import base.BaseApp;
import base.DWSBaseApp;
import bean.TableProcessDim;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import io.debezium.data.Json;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import util.FlinkSourceUtil;
import util.HBaseUtil;

/**
 * @基本功能:   公共维度层实时同步和数据导入
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-13 16:05:48
 **/

public class DimAPP extends BaseApp {
    public static void main(String[] args) throws Exception {
//        启动程序
        new DimAPP().start(
                10011,
                4,
                "dim_APP"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
//        TODO  1、读取维度层配置表数据
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("b1uemusic_dim_config", "dim_conf");
        DataStreamSource<String> confDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource").setParallelism(1);
//        confDS.print();

//        TODO  2、转换数据类型
        SingleOutputStreamOperator<TableProcessDim> dimConf = confDS.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String jsonStr) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                String op = jsonObject.getString("op");
                TableProcessDim result;
                if (op.equals("d")) {
                    result = jsonObject.getObject("before", TableProcessDim.class);
                } else {
                    result = jsonObject.getObject("after", TableProcessDim.class);
                }
                result.setOp(op);
                return result;
            }
        });

//        TODO  3、根据配置在HBase创建 /修改表
        dimConf.process(new ProcessFunction<TableProcessDim, TableProcessDim>() {
//            设置HBase连接
            private Connection hbaseconnection;

//            初始化HBaase连接
            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseconnection = HBaseUtil.getHBaseConnection();
            }

//            程序结束，释放连接
            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConnection(hbaseconnection);
            }

//            向HBase进行维度表的增删改
            @Override
            public void processElement(TableProcessDim dimconf, ProcessFunction<TableProcessDim, TableProcessDim>.Context ctx, Collector<TableProcessDim> out) throws Exception {
//                获取数据操作类型
                String op = dimconf.getOp();
                String[] columns = dimconf.getSinkColumns().split(",");
                if (op.equals("d")) {
//                    判断，如果数据类型是删除就向HBase删除该维度表
                    HBaseUtil.dropTable(hbaseconnection, "B1ueMusic_DIM", dimconf.getSinkTable());
                } else if (op.equals("r") || op.equals("c")) {
//                    如果类型为全量读取和创建就向HBase创建该维度表
                    HBaseUtil.createTable(hbaseconnection, "B1ueMusic_DIM", dimconf.getSinkTable(), columns);
                } else if (op.equals("u")) {
//                    如果类型为更改就向HBase删除并重新创建该维度表
                    HBaseUtil.dropTable(hbaseconnection, "B1ueMusic_DIM", dimconf.getSinkTable());
                    HBaseUtil.createTable(hbaseconnection, "B1ueMusic_DIM", dimconf.getSinkTable(), columns);
                }
                out.collect(dimconf);
            }
        });

//        TODO  4、将数据流进行广播
//        TODO  5、读取业务数据
//        TODO  6、联合广播流，过滤出维度表数据
//        TODO  7、写入HBase维度表
//        TODO  8、执行程序
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}