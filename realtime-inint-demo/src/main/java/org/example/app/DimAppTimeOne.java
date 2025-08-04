package org.example.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DimAppTimeOne {
    public static void main(String[] args) throws Exception {
        // 在后台线程启动定时关闭任务
        startShutdownTimer();

        // 创建并执行Flink作业
        runFlinkJob();
    }

    private static void startShutdownTimer() {
        Thread shutdownThread = new Thread(() -> {
            try {
                // 设置北京时区（Asia/Shanghai）
                ZoneId beijingZone = ZoneId.of("Asia/Shanghai");

                // 创建北京时间格式器
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                // 获取当前北京时间
                ZonedDateTime nowBeijing = ZonedDateTime.now(beijingZone);
                System.out.println("当前北京时间: " + nowBeijing.format(formatter));

                // 设置目标关闭时间（2025年8月4日 09:15:00 北京时间）
                ZonedDateTime targetDateTime = ZonedDateTime.of(
                        2025, 8, 4,    // 年、月、日
                        9, 20, 0, 0,  // 时、分、秒、纳秒
                        beijingZone    // 时区
                );

                // 计算时间差（秒）
                long delaySeconds = Duration.between(nowBeijing, targetDateTime).getSeconds();

                // 检查是否已过目标时间
                if (delaySeconds <= 0) {
                    System.out.println("目标时间已过，程序立即退出");
                    System.exit(0);
                    return;
                }

                System.out.printf("程序将在北京时间 %s 自动关闭%n", targetDateTime.format(formatter));
                System.out.printf("距离关闭还有: %d 秒%n", delaySeconds);

                // 创建定时线程池（使用单线程）
                ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

                // 安排关闭任务（使用秒为单位）
                scheduler.schedule(() -> {
                    System.out.println("已到达预设关闭时间（北京时间），程序即将关闭！");
                    System.exit(0);
                }, delaySeconds, TimeUnit.SECONDS);

                // 等待直到关闭时间到达
                try {
                    Thread.sleep(delaySeconds * 1000L + 1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } finally {
                // 确保线程池被关闭
                System.out.println("定时任务线程结束");
            }
        });

        // 设置为守护线程，不会阻止JVM退出
        shutdownThread.setDaemon(true);
        shutdownThread.start();
    }

    private static void runFlinkJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 添加数据源
        env.addSource(new SourceFunction<String>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isRunning) {
                    ctx.collect("Data: " + System.currentTimeMillis());
                    Thread.sleep(5000); // 每5秒产生一条数据
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        })
                // 数据处理
                .map(value -> "Processed: " + value)
                // 输出结果
                .print();

        // 执行Flink作业
        System.out.println("Flink作业开始执行...");
        env.execute("Time-Based Shutdown Demo");
    }
}