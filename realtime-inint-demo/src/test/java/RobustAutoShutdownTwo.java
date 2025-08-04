import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class RobustAutoShutdownTwo {
    private static final ZoneId BEIJING_ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        // 设置目标关闭时间 (2025年8月8日北京时间00:00:00)
        final ZonedDateTime targetTime = ZonedDateTime.of(2025, 8, 1, 14, 55, 0, 0, BEIJING_ZONE);

        // 获取程序启动时的实际北京时间
        final ZonedDateTime programStartTime = ZonedDateTime.now(BEIJING_ZONE);
        System.out.println("程序启动时间 (北京时间): " + programStartTime.format(TIME_FORMAT));
        System.out.println("目标关闭时间 (北京时间): " + targetTime.format(TIME_FORMAT));

        // 计算初始延迟时间（基于程序启动时间）
        final long initialDelayNanos = Duration.between(programStartTime, targetTime).toNanos();

        // 检查目标时间是否有效
        if (initialDelayNanos <= 0) {
            System.out.println("目标时间已过，程序立即退出");
            System.exit(0);
        }

        // 创建定时线程池
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);

        // 记录真实运行时间（不受系统时钟更改影响）
        final AtomicLong elapsedNanos = new AtomicLong(0);
        final long startNanoTime = System.nanoTime();

        // 安排关闭任务（基于纳秒级相对时间）
        scheduler.schedule(() -> {
            System.out.println("\n已到达预设关闭时间 (北京时间)，程序即将关闭！");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    System.err.println("警告：部分任务未能优雅结束");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.exit(0);
        }, initialDelayNanos, TimeUnit.NANOSECONDS);

        // 时间篡改检测线程（每分钟检查一次）
        scheduler.scheduleAtFixedRate(() -> {
            // 计算真实经过的时间（不受系统时钟影响）
            long actualElapsedNanos = System.nanoTime() - startNanoTime;
            elapsedNanos.set(actualElapsedNanos);

            // 检测系统时间是否被篡改
            ZonedDateTime currentSystemTime = ZonedDateTime.now(BEIJING_ZONE);
            ZonedDateTime expectedSystemTime = programStartTime.plusNanos(actualElapsedNanos);

            long timeDiffSeconds = Math.abs(Duration.between(currentSystemTime, expectedSystemTime).getSeconds());

            if (timeDiffSeconds > 60) {
                System.err.println("\n警告：系统时间可能被篡改！实际时间差: " + timeDiffSeconds + "秒");
                System.err.println("当前系统时间: " + currentSystemTime.format(TIME_FORMAT));
                System.err.println("预期系统时间: " + expectedSystemTime.format(TIME_FORMAT));
            }
        }, 1, 1, TimeUnit.MINUTES);

        // 剩余时间显示线程（每秒更新一次）
        scheduler.scheduleAtFixedRate(() -> {
            long remainingNanos = initialDelayNanos - elapsedNanos.get();

            if (remainingNanos <= 0) {
                System.out.println("关闭倒计时: 00:00:00");
                return;
            }

            Duration remaining = Duration.ofNanos(remainingNanos);
            long days = remaining.toDays();
            long hours = remaining.toHours() % 24;
            long minutes = remaining.toMinutes() % 60;
            long seconds = remaining.getSeconds() % 60;

            if (days > 0) {
                System.out.printf("关闭倒计时: %d天 %02d:%02d:%02d%n", days, hours, minutes, seconds);
            } else {
                System.out.printf("关闭倒计时: %02d:%02d:%02d%n", hours, minutes, seconds);
            }
        }, 0, 1, TimeUnit.SECONDS);

        // 等待程序结束
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("主线程被意外中断");
        }
    }
}