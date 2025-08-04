import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.geom.AffineTransform;
import java.util.Random;

public class CatChaseBall extends JPanel implements ActionListener {
    // 小猫位置和小球位置
    private double catX = 300, catY = 300;
    private double ballX = 500, ballY = 200;
    private double catAngle = 0; // 小猫的朝向角度

    private final Timer timer;
    private final Random random = new Random();

    public CatChaseBall() {
        timer = new Timer(30, this); // 每30毫秒更新一次动画
        timer.start();
    }

    @Override
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);
        Graphics2D g2d = (Graphics2D) g;
        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

        // 绘制背景
        g2d.setColor(new Color(230, 255, 230));
        g2d.fillRect(0, 0, getWidth(), getHeight());

        // 绘制草地
        g2d.setColor(new Color(100, 200, 100));
        for (int i = 0; i < 100; i++) {
            int x = random.nextInt(getWidth());
            int y = getHeight() - random.nextInt(30);
            g2d.fillOval(x, y, 3, 8);
        }

        // 绘制小球
        g2d.setColor(Color.RED);
        g2d.fillOval((int)ballX - 15, (int)ballY - 15, 30, 30);
        g2d.setColor(Color.BLACK);
        g2d.drawOval((int)ballX - 15, (int)ballY - 15, 30, 30);

        // 保存当前变换
        AffineTransform oldTransform = g2d.getTransform();

        // 旋转并绘制小猫（朝向小球）
        g2d.translate(catX, catY);
        g2d.rotate(catAngle);

        // 绘制小猫身体
        g2d.setColor(Color.ORANGE);
        g2d.fillOval(-20, -20, 40, 30); // 身体

        // 绘制小猫头部
        g2d.fillOval(-35, -30, 30, 30); // 头

        // 绘制耳朵
        g2d.setColor(Color.PINK);
        int[] xPoints = {-35, -25, -30};
        int[] yPoints = {-35, -35, -50};
        g2d.fillPolygon(xPoints, yPoints, 3); // 左耳
        int[] xPoints2 = {-15, -5, -10};
        g2d.fillPolygon(xPoints2, yPoints, 3); // 右耳

        // 绘制眼睛
        g2d.setColor(Color.BLACK);
        g2d.fillOval(-30, -25, 6, 8); // 左眼
        g2d.fillOval(-20, -25, 6, 8); // 右眼

        // 绘制胡须
        g2d.drawLine(-20, -18, -35, -15);
        g2d.drawLine(-20, -16, -35, -12);
        g2d.drawLine(-20, -14, -35, -9);

        // 绘制尾巴
        g2d.setColor(Color.ORANGE);
        g2d.drawArc(25, -15, 40, 20, 0, 180);

        // 绘制腿
        g2d.fillRect(5, 10, 5, 15); // 右后腿
        g2d.fillRect(-10, 10, 5, 15); // 左后腿
        g2d.fillRect(-25, -5, 5, 15); // 左前腿
        g2d.fillRect(-5, -5, 5, 15); // 右前腿

        // 恢复原始变换
        g2d.setTransform(oldTransform);

        // 显示状态
        g2d.setColor(Color.BLUE);
        g2d.drawString("小猫追小球动画", 10, 20);
        g2d.drawString("小球位置: (" + (int)ballX + ", " + (int)ballY + ")", 10, 40);
        g2d.drawString("小猫位置: (" + (int)catX + ", " + (int)catY + ")", 10, 60);
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        // 随机移动小球
        ballX += random.nextInt(11) - 5; // -5到+5之间的随机数
        ballY += random.nextInt(11) - 5;

        // 确保小球在窗口内
        if (ballX < 20) ballX = 20;
        if (ballX > getWidth() - 40) ballX = getWidth() - 40;
        if (ballY < 20) ballY = 20;
        if (ballY > getHeight() - 80) ballY = getHeight() - 80;

        // 计算小猫朝向小球的方向
        double dx = ballX - catX;
        double dy = ballY - catY;
        catAngle = Math.atan2(dy, dx);

        // 小猫向小球移动
        double speed = 4;
        catX += speed * Math.cos(catAngle);
        catY += speed * Math.sin(catAngle);

        repaint();
    }

    public static void main(String[] args) {
        JFrame frame = new JFrame("小猫追小球动画");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(800, 600);
        frame.add(new CatChaseBall());
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);
    }
}