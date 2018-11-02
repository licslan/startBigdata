package flinkplay;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author  Weilin Huang
 * flink word count demo
 *
 * Linux  服务器上面提交任务方式
 * bin/flink \
 * run -c 具体哪个类 \
 * path/to/xx-0.1.jar  (jar名字)
 *
 * */
public class flinkwc {

    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.readTextFile("D:\\PROJECT-HWLING\\Now-ing\\scala_spark_flink_study\\startBigData\\src\\main\\dataPool\\kv1.txt");

        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);

        counts.writeAsCsv("D:\\PROJECT-HWLING\\Now-ing\\scala_spark_flink_study\\startBigData\\src\\main\\dataPool\\xx.txt", "\n", " ");


    }
    // User-defined functions
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
