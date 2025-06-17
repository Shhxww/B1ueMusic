package base;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @基本功能:   Flink基础程序
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-13 16:49:58
 **/

public abstract class BaseApp {

    public void start(int port, int parallelism, String ck_path) throws Exception {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.flink", "warn");
//        TODO 1、 设置初始环境
//        1.1 获取流处理环境，并指定本地测试时启动 WebUI 所绑定的端口、启用火焰图
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, port);
        conf.setBoolean(RestOptions.ENABLE_FLAMEGRAPH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
//        1.2 设置程序全局并行度
        env.setParallelism(parallelism);
//        1.3   设置设置操作 Hadoop 的用户名为 Hadoop 超级用户 root
        System.setProperty("HADOOP_USER_NAME", "root");
//        1.4   创建动态表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        1.5   本地测试使用，上传服务器要注释
        System.setProperty("hadoop.home.dir", "D:\\Hadoop");

//            TODO  2、配置检查点、重启策略
//        2.1   启用检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        2.2   设置检查点超时时间
        checkpointConfig.setCheckpointTimeout(60000L);
//        2.3   设置检查点间隔时间
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
//        2.4   设置检查点在任务结束后进行保存，及其保存路径(保存在hdfs上要指定有权限的用户)
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointStorage("hdfs://node1:8020/Flink_checkpoint/"+ck_path);
//        2.5   设置（对齐超过10s启用）非对齐精准一次检查点
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(30));
//        2.6   设置重启策略（每3s重启一次，30天内仅能重启三次）
        env.setRestartStrategy(RestartStrategies.failureRateRestart(1, Time.days(3),Time.seconds(3)));

//        TODO  3、设置脏数据流、迟到数据流标签
        OutputTag<String> dirtyTag = new OutputTag<String>("BM_Dirty") {};
        OutputTag<String> lateTag = new OutputTag<String>("BM_Late") {};

//        TODO  4、进行数据处理
        handle(env,tEnv,dirtyTag,lateTag);

    }

    /**
     * 处理逻辑
     * @param env   数据流环境
     * @param tEnv  动态表环境
     * @param Dirty 脏数据流
     * @param Late  迟到流
     * @throws Exception
     */
    public abstract void handle(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tEnv,
            OutputTag<String> Dirty,
            OutputTag<String> Late
    ) throws Exception;


}
