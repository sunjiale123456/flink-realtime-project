package org.example.function;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class flinkThree {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        // 2. 异步执行获取 JobClient
        JobClient jobClient = env.executeAsync("MyJob");

        // 1. 构建数据流
        env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true){
                    sourceContext.collect(""+System.currentTimeMillis());
                    Thread.sleep(5000L);
                }

            }

            @Override
            public void cancel() {

            }
        }).print();


        env.execute();

        // 3. 启动定时关闭（传入JobClient）
        startShutdownTimer(jobClient);

//        // 4. 主线程保持运行（YARN模式）
//        if (isYarnClusterMode(env)) {
//            Thread.sleep(Long.MAX_VALUE);
//        } else {
//            jobClient.getJobExecutionResult().get(); // 本地模式
//        }
    }

    private static void startShutdownTimer(JobClient jobClient) {
        ZoneId zone = ZoneId.of("Asia/Shanghai");
        ZonedDateTime targetTime = ZonedDateTime.of(2025, 8, 7, 14, 18, 0, 0, zone);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            if (ZonedDateTime.now(zone).isAfter(targetTime)) {
                try {
                    // 关键步骤1: 取消作业
                    if (jobClient.getJobStatus().get() == JobStatus.RUNNING) {
                        jobClient.cancel().get(1, TimeUnit.MINUTES);
                    }

                    // 关键步骤2: 终止YARN应用
                    String appId = System.getenv("APPLICATION_ID");
                    System.out.println("appId: "+appId);
                    if (appId != null) {
                        Runtime.getRuntime().exec("yarn application -kill " + appId);
                    }

                    // 关键步骤3: 退出客户端
                    System.exit(0);
                } catch (Exception e) {
                    System.err.println("关闭失败: " + e.getMessage());
                    System.exit(1);
                }
            }
        }, 0, 30, TimeUnit.SECONDS);
    }
}
