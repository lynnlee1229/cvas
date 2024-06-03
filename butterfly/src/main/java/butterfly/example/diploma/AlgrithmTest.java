package butterfly.example.diploma;

import org.locationtech.jts.algorithm.PointLocator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * @author Lynn Lee
 * @date 2024/1/28
 **/
public class AlgrithmTest {
    public static void main(String[] args) {
        //创建4个多边形
        Coordinate[] coordinates1 = new Coordinate[]{new Coordinate(0, 0), new Coordinate(0, 1), new Coordinate(1, 2), new Coordinate(2, 1), //该点在union后会被消去
                new Coordinate(2, 0), new Coordinate(0, 0),};
        Coordinate[] coordinates2 = new Coordinate[]{new Coordinate(1, 0), new Coordinate(1, 1), //该点在union后会被消去
                new Coordinate(2, 2), new Coordinate(3, 1), new Coordinate(3, 0), new Coordinate(1, 0)};
        Coordinate[] coordinates3 = new Coordinate[]{new Coordinate(0, 0), new Coordinate(1, 0), new Coordinate(2, 0), new Coordinate(2, -1), new Coordinate(0, -1), new Coordinate(0, 0),};
        Coordinate[] coordinates4 = new Coordinate[]{new Coordinate(1, 0), new Coordinate(2, 0), new Coordinate(3, 0), new Coordinate(3, -1), new Coordinate(1, -1), new Coordinate(1, 0)};

        GeometryFactory gf = new GeometryFactory();
        Geometry g1 = gf.createPolygon(coordinates1);
        Geometry g2 = gf.createPolygon(coordinates2);
        Geometry g3 = gf.createPolygon(coordinates3);
        Geometry g4 = gf.createPolygon(coordinates4);
        //两两合并
        Geometry union = g1.union(g2);
        Geometry union2 = union.union(g3);
        Geometry union3 = union2.union(g4);

        //多个合并,设置多边形数组，再通过循环依次叠加各个多边形
        Geometry[] geos = new Geometry[]{g1, g2, g3, g4};
        Geometry allunion = geos[0];
        for (int i = 1; i < geos.length; i++) {
            allunion = allunion.union(geos[i]);
        }
//        System.out.println(union);
//        System.out.println(union2);
        System.out.println(union3);
        System.out.println(allunion);
        System.out.println(union3.equals(allunion));

        //缓冲区建立
        Geometry g3buffer = g3.buffer(1);                 //对第三个多边形加缓冲区
        Geometry allunionbuffer = allunion.buffer(1);     //对全部合并后的多边形加缓冲区
        System.out.println(g3buffer);
        System.out.println(allunionbuffer);

        //点是否在多边形内判断(by PointLocator)
        Coordinate point1 = new Coordinate(1, 1);
        PointLocator a = new PointLocator();
        if (a.intersects(point1, allunion)) System.out.println("point1:" + "该点在多边形内");
        else System.out.println("point1:" + "该点不在多边形内");

        Coordinate point2 = new Coordinate(5, 5);
        PointLocator b = new PointLocator();
        boolean p2 = b.intersects(point2, allunion);
        if (p2) System.out.println("point2:" + "该点在多边形内");
        else System.out.println("point2:" + "该点不在多边形内");
    }
}