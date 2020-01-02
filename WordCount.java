import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

/**
 * Implements a streaming windowed version of the "WordCount" program.
 *
 * <p>This program connects to a server socket and reads strings from the socket.
 * The easiest way to try this out is to open a text server (at port 12345)
 * using the <i>netcat</i> tool via
 * <pre>
 * nc -l 12345
 * </pre>
 * and run this example with the hostname and the port as arguments.
 */
@SuppressWarnings("serial")
public class WordCount {

    public static void main(String[] args) throws Exception {

        final String inputFile;
        final String outputFile;

        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            inputFile = params.get("input");
            outputFile = params.get("output");
        } catch (Exception e) {
            System.err.println("Input output files not specified. Please run 'WordCount "
                    + "--input <path-to-input-file> --output <path-to-output-file>'");
            return;
        }

        // get the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataSet<String> text = env.readTextFile(inputFile);

        // parse the data, group it, window it, and aggregate the counts
        DataSet<Tuple2<String, Integer>> wordCounts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) {
                for (String word : value.toLowerCase().split("\\s")) {
                    Pattern pattern = Pattern.compile("(?<!\\S)\\p{Alpha}+(?!\\S)");
                    Matcher matcher = pattern.matcher(word);
                    if (matcher.matches()) {
                        collector.collect(new Tuple2<String, Integer>(word, 1));
                    }
                }
            }
        }).groupBy(0).sum(1);

        // print the results
        wordCounts.writeAsCsv(outputFile, "\n", String.valueOf(","), OVERWRITE);
        env.execute("Socket Window WordCount");
    }
}