import javax.swing.*;
import javax.swing.Timer;
import java.awt.*;
import java.awt.event.*;
import java.awt.geom.*;
import java.util.*;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class BallSplitAnimation extends JPanel implements ActionListener {
    private static final int WIDTH = 800;
    private static final int HEIGHT = 600;
    private static final int CIRCLE_RADIUS = 250;
    private static final int INITIAL_BALL_RADIUS = 20;
    private static final int MAX_BALLS = 300; // 防止内存溢出

    private Point2D.Double circleCenter;
    private final CopyOnWriteArrayList<Ball> balls = new CopyOnWriteArrayList<>();
    private Timer timer;
    private long startTime;
    private final long DURATION = 60000; // 60秒

    public BallSplitAnimation() {
        circleCenter = new Point2D.Double(WIDTH / 2.0, HEIGHT / 2.0);

        // 添加初始球
        addBallAtRandomPosition();

        timer = new Timer(20, this);
        startTime = System.currentTimeMillis();
        timer.start();
    }

    // 在圆内随机位置添加一个新球
    private void addBallAtRandomPosition() {
        if (balls.size() >= MAX_BALLS) return; // 防止创建过多球

        Random rand = new Random();
        double angle = rand.nextDouble() * 2 * Math.PI;
        double distance = rand.nextDouble() * (CIRCLE_RADIUS - INITIAL_BALL_RADIUS - 10);

        double x = circleCenter.x + distance * Math.cos(angle);
        double y = circleCenter.y + distance * Math.sin(angle);

        // 给球一个随机的初始速度
        double vx = (rand.nextDouble()*10 - 0.5) * 4;
        double vy = (rand.nextDouble()*10 - 0.5) * 4;

        balls.add(new Ball(x, y, vx, vy, INITIAL_BALL_RADIUS, getRandomColor()));
    }

    // 生成随机颜色
    private Color getRandomColor() {
        Random rand = new Random();
        return new Color(rand.nextFloat(), rand.nextFloat(), rand.nextFloat());
    }

    @Override
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);
        Graphics2D g2d = (Graphics2D) g;
        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

        // 绘制灰色背景
        g2d.setColor(new Color(240, 240, 240));
        g2d.fillRect(0, 0, getWidth(), getHeight());

        // 绘制圆形边界
        g2d.setColor(Color.DARK_GRAY);
        g2d.setStroke(new BasicStroke(3));
        g2d.drawOval(
                (int)(circleCenter.x - CIRCLE_RADIUS),
                (int)(circleCenter.y - CIRCLE_RADIUS),
                CIRCLE_RADIUS * 2,
                CIRCLE_RADIUS * 2
        );

        // 绘制所有球
        for (Ball ball : balls) {
            g2d.setColor(ball.color);
            g2d.fillOval(
                    (int)(ball.x - ball.radius),
                    (int)(ball.y - ball.radius),
                    (int)(ball.radius * 2),
                    (int)(ball.radius * 2)
            );

            // 绘制小球轨迹
            if (ball.radius < 10) {
                g2d.setColor(new Color(150, 150, 150, 100));
                g2d.fillOval((int)(ball.x - ball.radius/2), (int)(ball.y - ball.radius/2),
                        (int)ball.radius, (int)ball.radius);
            }
        }

        // 显示球的数量和剩余时间
        long elapsed = System.currentTimeMillis() - startTime;
        long remaining = (DURATION - elapsed) / 1000;
        if (remaining < 0) remaining = 0;

        g2d.setColor(Color.BLACK);
        g2d.setFont(new Font("Arial", Font.BOLD, 16));
        g2d.drawString("球的数量: " + balls.size(), 10, 20);
        g2d.drawString("剩余时间: " + remaining + "秒", 10, 45);
        g2d.drawString("按空格键添加新球", 10, 70);

        // 结束时显示消息
        if (elapsed >= DURATION) {
            g2d.setFont(new Font("Arial", Font.BOLD, 40));
            g2d.setColor(new Color(200, 0, 0, 180));
            String message = "时间到! 最终球数: " + balls.size();
            int msgWidth = g2d.getFontMetrics().stringWidth(message);
            g2d.drawString(message, (WIDTH - msgWidth) / 2, HEIGHT / 2);
        }
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        long elapsed = System.currentTimeMillis() - startTime;
        if (elapsed >= DURATION) {
            timer.stop();
            repaint();
            return;
        }

        // 创建临时列表存储需要分裂的球
        List<Ball> ballsToSplit = new ArrayList<>();

        // 更新所有球的位置
        for (Ball ball : balls) {
            ball.move();

            // 检测是否碰到圆形边界
            double distance = circleCenter.distance(ball.x, ball.y);
            if (distance + ball.radius >= CIRCLE_RADIUS) {
                // 碰到边界，标记需要分裂
                ballsToSplit.add(ball);
            }
        }

        // 处理需要分裂的球
        for (Ball ball : ballsToSplit) {
            // 碰到边界，分裂成两个球
            if (balls.size() < MAX_BALLS) {
                splitBall(ball);
            }
            balls.remove(ball); // 移除原球
        }

        // 随机添加新球（几率较低）
        if (balls.size() < 10 && Math.random() < 0.02) {
            addBallAtRandomPosition();
        }

        repaint();
    }

    // 将球分裂成两个
    private void splitBall(Ball original) {
        Random rand = new Random();

        // 第一个新球
        double angle1 = Math.atan2(original.vy, original.vx) + (rand.nextDouble() - 0.5) * Math.PI/2;
        double speed1 = Math.sqrt(original.vx*original.vx + original.vy*original.vy) * (0.5 + rand.nextDouble() * 0.5);
        balls.add(new Ball(
                original.x, original.y,
                Math.cos(angle1) * speed1,
                Math.sin(angle1) * speed1,
                original.radius * 0.7,
                getRandomColor()
        ));

        // 第二个新球
        double angle2 = Math.atan2(original.vy, original.vx) - (rand.nextDouble() - 0.5) * Math.PI/2;
        double speed2 = Math.sqrt(original.vx*original.vx + original.vy*original.vy) * (0.5 + rand.nextDouble() * 0.5);
        balls.add(new Ball(
                original.x, original.y,
                Math.cos(angle2) * speed2,
                Math.sin(angle2) * speed2,
                original.radius * 0.7,
                getRandomColor()
        ));
    }

    // 球对象类
    private static class Ball {
        double x, y;           // 位置
        double vx, vy;         // 速度
        double radius;         // 半径
        Color color;           // 颜色

        public Ball(double x, double y, double vx, double vy, double radius, Color color) {
            this.x = x;
            this.y = y;
            this.vx = vx;
            this.vy = vy;
            this.radius = radius;
            this.color = color;
        }

        // 移动球
        public void move() {
            x += vx;
            y += vy;

            // 添加轻微的随机性使运动更自然
            if (Math.random() < 0.05) {
                vx += (Math.random() - 0.5) * 0.2;
                vy += (Math.random() - 0.5) * 0.2;
            }
        }
    }

    public static void main(String[] args) {
        JFrame frame = new JFrame("球分裂动画 - 60秒倒计时");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(WIDTH, HEIGHT);

        BallSplitAnimation animation = new BallSplitAnimation();
        frame.add(animation);

        // 添加键盘监听器，按空格键添加新球
        frame.addKeyListener(new KeyAdapter() {
            @Override
            public void keyPressed(KeyEvent e) {
                if (e.getKeyCode() == KeyEvent.VK_SPACE) {
                    animation.addBallAtRandomPosition();
                }
            }
        });

        frame.setLocationRelativeTo(null);
        frame.setFocusable(true); // 确保窗口能接收键盘事件
        frame.setVisible(true);
    }
}