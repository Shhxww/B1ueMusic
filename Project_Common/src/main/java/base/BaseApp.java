package base;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
//        1.1 获取流处理环境，并指定本地测试时启动 WebUI 所绑定的端口
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
//        1.2 设置程序全局并行度
        env.setParallelism(parallelism);
//        1.3   设置设置操作 Hadoop 的用户名为 Hadoop 超级用户 root
        System.setProperty("HADOOP_USER_NAME", "root");
//        1.4   创建动态表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

//        TODO 2、 配置检查点、重启策略
//        2.1   启用检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        2.2   设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        2.3   设置检查点间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        2.4   设置检查点在任务结束后进行保存，及其保存路径(保存在hdfs上要指定有权限的用户)
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://node1:8020/Flink_checkpoint/"+ck_path);
//        2.5   设置重启策略（每3s重启一次，30天内仅能重启三次）
        env.setRestartStrategy(RestartStrategies.failureRateRestart(1, Time.days(3),Time.seconds(3)));
//        2.6   进行处理
        handle(env,tEnv);

    }

    public abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) throws Exception;
}
