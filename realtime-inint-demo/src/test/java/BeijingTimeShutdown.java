import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class BeijingTimeShutdown {
    public static void main(String[] args) {
        // 设置北京时区 (Asia/Shanghai)
        ZoneId beijingZone = ZoneId.of("Asia/Shanghai");

        // 创建北京时间格式器
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 获取当前北京时间
        ZonedDateTime nowBeijing = ZonedDateTime.now(beijingZone);
        System.out.println("当前北京时间: " + nowBeijing.format(formatter));

        // 设置目标关闭时间 (2025年8月8日 00:00:00 北京时间)
        ZonedDateTime targetDateTime = ZonedDateTime.of(
                2025, 8, 1,    // 年、月、日
                15, 15, 0, 0,    // 时、分、秒、纳秒
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

        // 计算初始剩余时间（天、小时、分钟）
//        long initialDays = delaySeconds / 86400;
//        long initialHours = (delaySeconds % 86400) / 3600;
//        long initialMinutes = (delaySeconds % 3600) / 60;
//        System.out.printf("程序将在北京时间 %s 自动关闭%n", targetDateTime.format(formatter));
//        System.out.printf("距离关闭还有: %d 天 %d 小时 %d 分钟%n",
//                initialDays, initialHours, initialMinutes);

        // 创建定时线程池
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        // 安排关闭任务（使用秒为单位）
        scheduler.schedule(() -> {
            System.out.println("已到达预设关闭时间（北京时间），程序即将关闭！");
            System.exit(0);
        }, delaySeconds, TimeUnit.SECONDS);

        // 添加安全关闭钩子
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            System.out.println("执行清理操作...");
//            scheduler.shutdownNow();
//        }));

        // 监控线程：每分钟打印一次剩余时间（使用Java 8兼容方式）
//        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
//        monitor.scheduleAtFixedRate(() -> {
//            Duration remaining = Duration.between(ZonedDateTime.now(beijingZone), targetDateTime);
//            if (remaining.isNegative()) {
//                // 如果已经超时，则取消监控任务
//                monitor.shutdownNow();
//                return;
//            }
//            // 将剩余时间转换为总秒数
//            long seconds = remaining.getSeconds();
//            long days = seconds / 86400;
//            long hours = (seconds % 86400) / 3600;
//            long minutes = (seconds % 3600) / 60;
//
//            // 获取当前北京时间并格式化
//            String currentTime = ZonedDateTime.now(beijingZone).format(DateTimeFormatter.ofPattern("HH:mm:ss"));
//            System.out.printf("[%s] 距离关闭还有: %d天 %d小时 %d分钟%n",
//                    currentTime, days, hours, minutes);
//        }, 0, 1, TimeUnit.MINUTES); // 延迟0分钟，每1分钟执行一次

        // 保持程序运行
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}