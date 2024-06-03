package butterfly.example.diploma;

import javax.swing.*;
import java.awt.*;

public class HilbertCurve extends JFrame {

    private int order;

    public HilbertCurve(int order) {
        this.order = order;
        setTitle("Hilbert Curve");
        setSize(600, 600);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);
    }

    @Override
    public void paint(Graphics g) {
        super.paint(g);
        drawHilbertCurve(g, order, 50, 50, 500, 0);
    }

    private void drawHilbertCurve(Graphics g, int order, int x, int y, int size, int direction) {
        if (order == 0) {
            g.drawLine(x, y, x + size, y);
            g.drawLine(x + size, y, x + size, y + size);
            g.drawLine(x + size, y + size, x, y + size);
        } else {
            size /= 2;
            order--;

            switch (direction) {
                case 0:
                    drawHilbertCurve(g, order, x, y, size, 1);
                    drawHilbertCurve(g, order, x + size, y, size, 0);
                    drawHilbertCurve(g, order, x + size, y + size, size, 0);
                    drawHilbertCurve(g, order, x, y + size, size, 3);
                    break;
                case 1:
                    drawHilbertCurve(g, order, x, y, size, 2);
                    drawHilbertCurve(g, order, x, y + size, size, 1);
                    drawHilbertCurve(g, order, x + size, y + size, size, 1);
                    drawHilbertCurve(g, order, x + size, y, size, 2);
                    break;
                case 2:
                    drawHilbertCurve(g, order, x, y + size, size, 3);
                    drawHilbertCurve(g, order, x + size, y + size, size, 2);
                    drawHilbertCurve(g, order, x + size, y, size, 2);
                    drawHilbertCurve(g, order, x, y, size, 3);
                    break;
                case 3:
                    drawHilbertCurve(g, order, x + size, y, size, 0);
                    drawHilbertCurve(g, order, x + size, y + size, size, 3);
                    drawHilbertCurve(g, order, x, y + size, size, 3);
                    drawHilbertCurve(g, order, x, y, size, 0);
                    break;
            }
        }
    }

    public static void main(String[] args) {
        int order = 4; // 设置Hilbert曲线的阶数
        SwingUtilities.invokeLater(() -> {
            HilbertCurve hilbertCurve = new HilbertCurve(order);
            hilbertCurve.setVisible(true);
        });
    }
}
