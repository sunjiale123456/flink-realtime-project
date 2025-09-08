

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.*;

public class DimAppTimer {

    public static void main(String[] args) throws Exception {

            // 1. 创建执行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(3);

            // 2. 构建数据流（模拟持续数据源）
            env.addSource(new SourceFunction<String>() {
                private volatile boolean isRunning = true;

                @Override
                public void run(SourceContext<String> ctx) throws Exception {
                    while (isRunning) {
                        ctx.collect("Data-" + System.currentTimeMillis());
                        Thread.sleep(5000); // 每5秒产生一条数据
                    }
                }

                @Override
                public void cancel() {
                    isRunning = false;
                }
            }).print(); // 直接打印输出

            // 3. 异步提交作业到YARN集群
            JobClient jobClient = env.executeAsync("TimedShutdownDemo");

            // 4. 启动定时关闭服务
            startShutdownService(jobClient, env);

            // 5. 保持主线程运行（YARN集群模式需要）
//            if (isYarnClusterMode(env)) {
//                System.out.println("主线程进入等待状态...");
//                Thread.sleep(Long.MAX_VALUE); // 永久阻塞
//            } else {
//                // 本地模式直接等待作业完成
//                jobClient.getJobExecutionResult().get();
//            }

    }

    /**
     * 启动定时关闭服务
     */
    private static void startShutdownService(JobClient jobClient, StreamExecutionEnvironment env) {
        ZoneId beijingZone = ZoneId.of("Asia/Shanghai");
        // 设置目标关闭时间（示例：2025-08-07 15:30:00 北京时间）
        ZonedDateTime targetTime = ZonedDateTime.of(2025, 8, 7, 14, 11, 0, 0, beijingZone);

        // 创建定时线程池（单线程，守护线程）
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "shutdown-monitor");
            t.setDaemon(true);
            return t;
        });

        // 每30秒检查一次时间
        scheduler.scheduleAtFixedRate(() -> {
            try {
                ZonedDateTime now = ZonedDateTime.now(beijingZone);
                long remaining = Duration.between(now, targetTime).getSeconds();

                if (remaining > 0) {
                    System.out.printf("[%s] 距离关闭还有 %d 秒%n", now, remaining);
                    return;
                }

                System.out.printf("[%s] ❤❤❤ 到达预设关闭时间，开始终止作业...%n", now);

                // 步骤1: 优雅取消Flink作业
                if (jobClient != null) {
                    JobStatus status = jobClient.getJobStatus().get(10, TimeUnit.SECONDS);
                    if (status == JobStatus.RUNNING) {
                        System.out.println("正在取消Flink作业...");
                        jobClient.cancel().get(1, TimeUnit.MINUTES);
                        }
                }

                // 步骤2: 强制终止YARN应用（双重保障）
                if (isYarnClusterMode(env)) {
                    terminateYarnApplication();
                }

                // 步骤3: 关闭定时器并退出程序
                scheduler.shutdownNow();
                System.out.println("定时关闭服务已停止");
                System.exit(0);

            } catch (Exception e) {
                System.err.println("定时关闭服务异常: " + e.getMessage());
                e.printStackTrace();
                System.exit(1);
            }
        }, 0, 30, TimeUnit.SECONDS);
    }

    /**
     * 强制终止YARN应用
     */
    private static void terminateYarnApplication() throws Exception {
        String appId = System.getenv("APPLICATION_ID");
        if (appId == null) {
            System.out.println("未获取到YARN应用ID，跳过强制终止");
            return;
        }

        System.out.println("正在终止YARN应用: " + appId);
        Process process = new ProcessBuilder("yarn", "application", "-kill", appId)
                .redirectErrorStream(true)
                .start();

        // 等待命令执行完成（最多30秒）
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            System.err.println("YARN终止命令超时");
            process.destroyForcibly();
        }

        // 检查返回值
        if (process.exitValue() != 0) {
            throw new RuntimeException("YARN终止命令执行失败");
        }
        System.out.println("YARN应用已终止");
    }

    /**
     * 判断是否为YARN集群模式
     */
    private static boolean isYarnClusterMode(StreamExecutionEnvironment env) {
        Configuration config = (Configuration) env.getConfiguration();
        String target = config.get(DeploymentOptions.TARGET);
        return target != null && target.toLowerCase().contains("yarn");
    }
}