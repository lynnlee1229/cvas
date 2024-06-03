package butterfly.example.diploma;

import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.List;

class QuadtreeNode {
    private static final int MAX_POINTS = 5;

    int x;
    int y;
    int width;
    int height;
    private QuadtreeNode[] children;
    private List<Point> points;

    public QuadtreeNode(int x, int y, int width, int height) {
        this.x = x;
        this.y = y;
        this.width = width;
        this.height = height;
        this.children = new QuadtreeNode[4];
        this.points = new ArrayList<>();
    }

    public void divide() {
        int subWidth = width / 2;
        int subHeight = height / 2;

        children[0] = new QuadtreeNode(x, y, subWidth, subHeight);
        children[1] = new QuadtreeNode(x + subWidth, y, subWidth, subHeight);
        children[2] = new QuadtreeNode(x, y + subHeight, subWidth, subHeight);
        children[3] = new QuadtreeNode(x + subWidth, y + subHeight, subWidth, subHeight);

        // 将节点中的点分配到子节点中
        for (Point point : points) {
            for (QuadtreeNode child : children) {
                if (child.contains(point)) {
                    child.addPoint(point);
                    break;
                }
            }
        }
        points.clear(); // 清空当前节点的点
    }

    public void addPoint(Point point) {
        points.add(point);

        // 如果当前节点的点数超过阈值，进行切分
        if (points.size() > MAX_POINTS) {
            if (children[0] == null) {
                divide();
            }
        }
    }

    public boolean contains(Point point) {
        return point.x >= x && point.x <= x + width && point.y >= y && point.y <= y + height;
    }

    public List<Point> getPoints() {
        return points;
    }

    public QuadtreeNode[] getChildren() {
        return children;
    }

    public void draw(Graphics g) {
        g.drawRect(x, y, width, height);

        for (QuadtreeNode child : children) {
            if (child != null) {
                child.draw(g);
            }
        }

        for (Point point : points) {
            g.setColor(Color.RED);
            g.fillOval(point.x - 2, point.y - 2, 4, 4);
        }
    }
}

class QuadtreePanel extends JPanel {
    private QuadtreeNode root;

    public QuadtreePanel() {
        root = new QuadtreeNode(0, 0, 800, 800);
        addRandomPoints(root, 1000);
        setPreferredSize(new Dimension(800, 800));

        addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                root.addPoint(e.getPoint());
                repaint();
            }
        });
    }

    private void addRandomPoints(QuadtreeNode node, int numPoints) {
        for (int i = 0; i < numPoints; i++) {
            int x = (int) (Math.random() * node.width) + node.x;
            int y = (int) (Math.random() * node.height) + node.y;
            node.addPoint(new Point(x, y));
        }
    }

    @Override
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);
        drawQuadtreeGrid(g, root);
    }

    private void drawQuadtreeGrid(Graphics g, QuadtreeNode node) {
        if (node.getChildren()[0] != null) {
            for (QuadtreeNode child : node.getChildren()) {
                drawQuadtreeGrid(g, child);
            }
        } else {
            g.setColor(Color.GRAY);
            g.drawRect(node.x, node.y, node.width, node.height);

            for (Point point : node.getPoints()) {
                g.setColor(Color.RED);
                g.fillOval(point.x - 2, point.y - 2, 4, 4);
            }
        }
    }
}

public class QuadtreeExample {
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame("Quadtree Example");
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.getContentPane().add(new QuadtreePanel());
            frame.pack();
            frame.setLocationRelativeTo(null);
            frame.setVisible(true);
        });
    }
}
