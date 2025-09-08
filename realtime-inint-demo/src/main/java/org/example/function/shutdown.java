package org.example.function;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class shutdown {
    public void close(){
        // 设置北京时区（Asia/Shanghai）
        ZoneId beijingZone = ZoneId.of("Asia/Shanghai");

        // 创建北京时间格式器
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 获取当前北京时间
        ZonedDateTime nowBeijing = ZonedDateTime.now(beijingZone);
        System.out.println("当前北京时间: " + nowBeijing.format(formatter));

        // 设置目标关闭时间（2025年8月1日 15:30:00 北京时间）
//        ZonedDateTime targetDateTime = ZonedDateTime.of(
//                2025, 8, 4,    // 年、月、日
//                15, 13, 0, 0,  // 时、分、秒、纳秒
//                beijingZone    // 时区
//        );
        ZonedDateTime targetDateTime = ZonedDateTime.now(beijingZone).plusMinutes(3);

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


        }, 0, 30, TimeUnit.SECONDS); // 初始延迟0秒，每30秒执行一次

    }
}
