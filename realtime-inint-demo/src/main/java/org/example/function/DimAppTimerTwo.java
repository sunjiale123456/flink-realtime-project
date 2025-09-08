package org.example.function;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DimAppTimerTwo {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 构建数据流
        env.addSource(new SourceFunction<String>() {
            private volatile boolean isRunning = true;
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isRunning) {
                    ctx.collect("1");
                    Thread.sleep(5000L);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).map(s -> s + "" + System.currentTimeMillis()).print();
        // 异步执行作业并获取JobClient
        JobClient jobClient = env.executeAsync("Timed Cancel Job");

        // 创建定时关闭线程（守护线程）
        timerClose(jobClient);

    }
    public static void timerClose(JobClient jobClient) {
        ZoneId beijingZone = ZoneId.of("Asia/Shanghai");
        ZonedDateTime targetDateTime = ZonedDateTime.of(
                2025, 8, 6, 17, 11, 0, 0, beijingZone);

        // 创建守护线程的定时器
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            try {
                ZonedDateTime currentTime = ZonedDateTime.now(beijingZone);
                long remainingSeconds = Duration.between(currentTime, targetDateTime).getSeconds();

                System.out.println("距离关闭还有: " + remainingSeconds + " 秒");

                // 只需检查是否超过目标时间
                if (currentTime.isAfter(targetDateTime) || currentTime.equals(targetDateTime)) {
                    System.out.println("❤❤❤❤️ 已到关闭时间，优雅取消Flink作业！");

                    if (jobClient != null) {
                        // 1. 检查作业状态
                        JobStatus status = jobClient.getJobStatus().get();

                        // 2. 只取消运行中的作业
                        if (status == JobStatus.RUNNING) {
                            System.out.println("正在取消作业...");

                            // 3. 带超时的取消操作
                            jobClient.cancel().get(30, TimeUnit.SECONDS);

                            // 4. 等待作业终止
                            while (!jobClient.getJobStatus().get().isGloballyTerminalState()) {
                                Thread.sleep(1000);
                            }
                            System.out.println("作业已终止");
                        } else {
                            System.out.println("作业已处于终止状态: " + status);
                        }
                    }

                    // 5. 最后关闭定时器
                    scheduler.shutdown();
                    System.out.println("定时器已关闭");
                }
            } catch (Exception e) {
                System.err.println("定时关闭异常: " + e.getMessage());
                e.printStackTrace();
            }
        }, 0, 30, TimeUnit.SECONDS);
    }

//    public static void timerClose(JobClient jobClient) {
//        // 设置北京时区（Asia/Shanghai）
//        ZoneId beijingZone = ZoneId.of("Asia/Shanghai");
//        // 设置目标关闭时间（2025年8月4日 13:55:00 北京时间）
//        ZonedDateTime targetDateTime = ZonedDateTime.of(
//                2025, 8, 6,    // 年、月、日
//                14, 53, 0, 0,  // 时、分、秒、纳秒
//                beijingZone    // 时区
//        );
//        // 创建定时线程池（使用单线程）
//        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
//
//        // 添加定时检测
//        scheduler.scheduleAtFixedRate(() -> {
//            try {
//                // 1. 获取当前北京时间
//                ZonedDateTime currentTime = ZonedDateTime.now(beijingZone);
//
//                // 2. 检查是否超过目标时间
//                if (currentTime.isAfter(targetDateTime) || currentTime.equals(targetDateTime)) {
//                    System.out.println("❤❤❤️ 已到关闭时间，优雅取消Flink作业！");
//
//                    // 3. 取消作业
//                    if (jobClient != null) {
//                        jobClient.cancel().get(); // 等待取消操作完成
//                    }
//
//                    // 4. 关闭定时器
//                    scheduler.shutdown();
//                } else {
//                    // 可选：打印剩余时间
//                    long remainingSeconds = Duration.between(currentTime, targetDateTime).getSeconds();
//                    System.out.println("距离关闭还有: " + remainingSeconds + " 秒");
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }, 0, 30, TimeUnit.SECONDS); // 初始延迟0秒，每30秒执行一次
//    }
}