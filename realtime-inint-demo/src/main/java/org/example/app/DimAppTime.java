package org.example.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import sun.net.www.protocol.http.HttpURLConnection;

import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DimAppTime {
    public static void main(String[] args) throws Exception {
        // 创建定时关闭线程（守护线程）
        Thread shutdownThread = new Thread(DimAppTime::timerClose);
        shutdownThread.setDaemon(true); // 设置为守护线程
        shutdownThread.start();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

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

        env.execute();
    }

    public static void timerClose() {
        // 设置北京时区（Asia/Shanghai）
        ZoneId beijingZone = ZoneId.of("Asia/Shanghai");

        // 创建北京时间格式器
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 获取当前北京时间
        ZonedDateTime nowBeijing = ZonedDateTime.now(beijingZone);
        System.out.println("当前北京时间: " + nowBeijing.format(formatter));

        // 设置目标关闭时间（2025年8月1日 15:30:00 北京时间）
        ZonedDateTime targetDateTime = ZonedDateTime.of(
                2025, 8, 4,    // 年、月、日
                15, 13, 0, 0,  // 时、分、秒、纳秒
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

        // 添加心跳机制（每30秒执行一次）
        scheduler.scheduleAtFixedRate(() -> {
            // 1. 获取当前北京时间
            ZonedDateTime currentTime = ZonedDateTime.now(beijingZone);

            // 2. 检查是否超过目标时间
            if (currentTime.isAfter(targetDateTime) || currentTime.equals(targetDateTime)) {
                System.out.println("❤❤❤️ 心跳检测: 已到关闭时间，立即关闭程序！");
                scheduler.shutdown();
                System.exit(0);
            }

            // 3. 计算剩余时间
            Duration remaining = Duration.between(currentTime, targetDateTime);

            // 4. 记录心跳日志
            System.out.printf("❤️ 心跳检测 | 当前时间: %s | 剩余时间: %d分%d秒%n",
                    currentTime.format(formatter),
                    remaining.toMinutes(),
                    remaining.getSeconds() % 60);

            // 5. 额外验证（可选）：检查时间是否被篡改
//            verifyTimeConsistency(scheduler,beijingZone);

        }, 0, 30, TimeUnit.SECONDS); // 初始延迟0秒，每30秒执行一次

        // 安排关闭任务（使用秒为单位）
        scheduler.schedule(() -> {
            System.out.println("已到达预设关闭时间（北京时间），程序即将关闭！");
            System.exit(0);
        }, delaySeconds, TimeUnit.SECONDS);

    }

    // 时间一致性验证（防止系统时间被篡改）
    private static void verifyTimeConsistency(ScheduledExecutorService scheduler,ZoneId beijingZone) {
        // 创建当前时间的备份
        ZonedDateTime now = ZonedDateTime.now(beijingZone);

        // 等待1秒后再次检查
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 获取新时间
        ZonedDateTime later = ZonedDateTime.now(beijingZone);

        // 计算实际经过时间（秒）
        long actualElapsed = Duration.between(now, later).getSeconds();

        // 正常情况下应该经过约1秒
        if (Math.abs(actualElapsed - 1) > 0.5) {
            System.err.println("⚠️ 时间异常: 检测到系统时间被篡改！");
            System.err.printf("实际经过时间: %.3f秒，预期1秒%n", actualElapsed);

            // 立即关闭程序
            System.err.println("⚠️ 异常处理: 立即关闭程序");
            scheduler.shutdown();
            System.exit(1);
        }
    }
    public void getHttpTime(){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        try {
            URL url = new URL("http://www.ntsc.ac.cn");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("HEAD");
            conn.setConnectTimeout(3000);
            long serverTime = conn.getHeaderFieldDate("Date", 0);
            if (serverTime > 0) {
                System.out.println(formatter.format(Instant.ofEpochMilli(serverTime).atZone(ZoneId.of("Asia/Shanghai"))));
            }
        } catch (Exception e) {
            System.out.println(formatter.format(ZonedDateTime.now(ZoneId.of("Asia/Shanghai"))));
            System.err.println("无法获取网络时间，使用本地时间: " + e.getMessage());
        }
    }
}