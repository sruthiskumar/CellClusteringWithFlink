import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Collection;

/**
 * This example implements a basic K-Means clustering algorithm.
 *
 * <p>K-Means is an iterative clustering algorithm and works as follows:<br>
 * K-Means is given a set of data points to be clustered and an initial set of <i>K</i> cluster centers.
 * In each iteration, the algorithm computes the distance of each data point to each cluster center.
 * Each point is assigned to the cluster center which is closest to it.
 * Subsequently, each cluster center is moved to the center (<i>mean</i>) of all points that have been assigned to it.
 * The moved cluster centers are fed into the next iteration.
 * The algorithm terminates after a fixed number of iterations (as in this implementation)
 * or if cluster centers do not (significantly) move in an iteration.<br>
 * This is the Wikipedia entry for the <a href="http://en.wikipedia.org/wiki/K-means_clustering">K-Means Clustering algorithm</a>.
 *
 * <p>This implementation works on two-dimensional data points. <br>
 * It computes an assignment of data points to cluster centers, i.e.,
 * each data point is annotated with the id of the final cluster (center) it belongs to.
 *
 *
 * <p>Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Data points are represented as two double values separated by a blank character.
 * Data points are separated by newline characters.<br>
 * For example <code>"1.2 2.3\n5.3 7.2\n"</code> gives two data points (x=1.2, y=2.3) and (x=5.3, y=7.2).
 * <li>Cluster centers are represented by an integer id and a point value.<br>
 * For example <code>"1 6.2 3.2\n2 2.9 5.7\n"</code> gives two centers (id=1, x=6.2, y=3.2) and (id=2, x=2.9, y=5.7).
 * </ul>
 *
 * <p>Usage: <code>KMeans --points &lt;path&gt; --centroids &lt;path&gt; --output &lt;path&gt; --iterations &lt;n&gt;</code><br>
 * If no parameters are provided, the program is run with default data from  and 10 iterations.
 *
 * <p>This example shows how to use:
 * <ul>
 * <li>Bulk iterations
 * <li>Broadcast variables in bulk iterations
 * <li>Custom Java objects (POJOs)
 * </ul>
 */
@SuppressWarnings("serial")
public class CellCluster {

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface

        // get input data:
        DataSet<Data> data= getDataFromCsv(params, env);


        // read the points and centroids from the provided paths or fall back to default data
        DataSet<Point> points = getPointDataSet(data);
        DataSet<Centroid> centroids = getCentroidDataSet(data, params);

        // set number of bulk iterations for CellCluster algorithm
        IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("iterations", 10));

        DataSet<Centroid> newCentroids = points
                // compute closest centroid for each point
                .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                // count and sum point coordinates for each centroid
                .map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator())
                // compute new centroids from point counts and coordinate sums
                .map(new CentroidAverager());

        // feed new centroids back into next iteration
        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

        DataSet<Tuple2<Integer, Point>> clusteredPoints = points
                // assign points to final clusters
                .map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

        // emit result
        if (params.has("output")) {
            clusteredPoints.writeAsCsv(params.get("output"), "\n", ",", FileSystem.WriteMode.OVERWRITE);

            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("KMeans Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            clusteredPoints.print();
        }
    }

    // *************************************************************************
    //     DATA SOURCE READING (POINTS AND CENTROIDS)
    // *************************************************************************

    private static DataSet<Point> getPointDataSet(DataSet<Data> dataSet) throws Exception {
        DataSet<Point> points = dataSet.flatMap(new FlatMapFunction<Data, Point>() {
            @Override
            public void flatMap(Data data, Collector<Point> collector) throws Exception {
                if(data.radio.equalsIgnoreCase("UMTS") || data.radio.equalsIgnoreCase("GSM")){
                    collector.collect(new Point(data.lon, data.lat));
                }
            }
        });

        return points;
    }

    private static DataSet<Centroid> getCentroidDataSet(DataSet<Data> dataSet, ParameterTool params) throws Exception {
        DataSet<Centroid> centroids = dataSet.flatMap(new FlatMapFunction<Data, Centroid>() {
            @Override
            public void flatMap(Data data, Collector<Centroid> collector) throws Exception {
                if(data.radio.equalsIgnoreCase("LTE")){
                    collector.collect(new Centroid(data.cell, data.lon, data.lat));
                }
            }
        });
        if (params.has("k")) {
            int count = Integer.parseInt(params.get("k"));
            return  centroids.first(count);
        }
        else {
            return centroids;
        }
    }

    private static DataSet<Data> getDataFromCsv(ParameterTool params, ExecutionEnvironment env) throws Exception {
        DataSet<Data> data = null;
        if (params.has("input")) {
            // read points from CSV file
            data = env.readCsvFile(params.get("input")).ignoreFirstLine()
                    .fieldDelimiter(",")
                    .pojoType(Data.class, "radio", "mcc", "net", "area", "cell", "unit", "lon", "lat",
                                    "range", "samples", "changeable", "created",	"updated",	"averageSignal")
                    .filter(new FilterFunction<Data>() {
                        @Override
                        public boolean filter(Data data) throws Exception {
                            if(params.has("mnc")) {
                                String networkOperators = params.get("mnc");
                                return networkOperators.contains(data.net.toString());
                            }
                            return true;
                        }
                    });
        }
        return data;
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    /**
     * A simple two-dimensional point.
     */
    public static class Point implements Serializable {

        public double lon, lat;

        public Point() {}

        public Point(double lon, double lat) {
            this.lon = lon;
            this.lat = lat;
        }

        public Point add(Point other) {
            lon += other.lon;
            lat += other.lat;
            return this;
        }

        public Point div(long val) {
            lon /= val;
            lat /= val;
            return this;
        }

        public double euclideanDistance(Point other) {
            return Math.sqrt((lon - other.lon) * (lon - other.lon) + (lat - other.lat) * (lat - other.lat));
        }

        public void clear() {
            lon = lat = 0.0;
        }

        @Override
        public String toString() {
            return lon + "," + lat;
        }
    }
    /**
     * The data from csv.
     */
    public static class Data implements Serializable {

        public String radio;
        public int cell;
        public Long mcc, net, area, range, samples, changeable, averageSignal, created, updated, unit;
        public double lon, lat;

        public Data() {}

        public Data(String radio, Long mcc, double lon, double lat, Long net, 
                            Long area, int cell, Long range, Long samples, Long changeable,
                    Long averageSignal, Long unit, Long created, Long updated) {
            this.radio = radio;
            this.mcc = mcc;
            this.lon = lon;
            this.lat = lat;
            this.net = net;
            this.area = area;
            this.cell = cell;
            this.range = range;
            this.samples = samples;
            this.changeable = changeable;
            this.averageSignal = averageSignal;
            this.unit = unit;
            this.created = created;
            this.updated = updated;

        }

        @Override
        public String toString() {
            return "Data{" +
                    "radio='" + radio + '\'' +
                    ", mcc=" + mcc +
                    ", net=" + net +
                    ", area=" + area +
                    ", cell=" + cell +
                    ", range=" + range +
                    ", samples=" + samples +
                    ", changeable=" + changeable +
                    ", averageSignal=" + averageSignal +
                    ", created=" + created +
                    ", updated=" + updated +
                    ", unit=" + unit +
                    ", lon=" + lon +
                    ", lat=" + lat +
                    '}';
        }
    }

    /**
     * A simple two-dimensional centroid, basically a point with an ID.
     */
    public static class Centroid extends Point {

        public int id;

        public Centroid() {}

        public Centroid(int id, double x, double y) {
            super(x, y);
            this.id = id;
        }

        public Centroid(int id, Point p) {
            super(p.lon, p.lat);
            this.id = id;
        }

        @Override
        public String toString() {
            return id + "," + super.toString();
        }
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /** Determines the closest cluster center for a data point. */
    @ForwardedFields("*->1")
    public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {
        private Collection<Centroid> centroids;

        /** Reads the centroid values from a broadcast variable into a collection. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Point> map(Point p) throws Exception {

            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            // check all cluster centers
            for (Centroid centroid : centroids) {
                // compute distance
                double distance = p.euclideanDistance(centroid);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.id;
                }
            }

            // emit a new record with the center id and the data point.
            return new Tuple2<>(closestCentroidId, p);
        }
    }

    /** Appends a count variable to the tuple. */
    @ForwardedFields("f0;f1")
    public static final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
            return new Tuple3<>(t.f0, t.f1, 1L);
        }
    }

    /** Sums and counts point coordinates. */
    @ForwardedFields("0")
    public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
            return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
        }
    }

    /** Computes new centroid from coordinate sum and count of points. */
    @ForwardedFields("0->id")
    public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

        @Override
        public Centroid map(Tuple3<Integer, Point, Long> value) {
            return new Centroid(value.f0, value.f1.div(value.f2));
        }
    }
}