
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.app.DimAppTime;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DimAppTimer {
    public static void main(String[] args) throws Exception {
        // 创建定时关闭线程（守护线程）
        Thread shutdownThread = new Thread(DimAppTimer::timerClose);
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
                10, 50, 0, 0,  // 时、分、秒、纳秒
                beijingZone    // 时区
        );

        // 计算时间差（秒）
        long delaySeconds = Duration.between(nowBeijing, targetDateTime).getSeconds();
        // 计算时间差（天）
        long delayDays = Duration.between(nowBeijing, targetDateTime).getSeconds()/(3600*24);

        // 检查是否已过目标时间
        if (delayDays <= 0) {
            System.out.println("目标时间已过，程序立即退出");
            System.exit(0);
            return;
        }

        System.out.printf("程序将在北京时间 %s 自动关闭%n", targetDateTime.format(formatter));
        System.out.printf("距离关闭还有: %d 秒%n", delaySeconds);
        System.out.printf("距离关闭还有: %d 天%n", delayDays);

        // 创建定时线程池（使用单线程）
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        // 安排关闭任务（使用秒为单位）
        scheduler.schedule(() -> {
            System.out.println("已到达预设关闭时间（北京时间），程序即将关闭！");
            System.exit(0);
        }, delayDays, TimeUnit.DAYS);

    }
}