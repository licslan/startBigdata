package flinkplay;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
/**
 * @author  Weilin Huang
 * flink page rank demo
 *
 * Linux  服务器上面提交任务方式
 * bin/flink \
 * run -c 具体哪个类 \
 * path/to/xx-0.1.jar  (jar名字)
 *
 * */
public class PageRank
{
//    private static final double DAMPENING_FACTOR = 0.85D;
//    private static final double EPSILON = 0.0001D;

    public static void main(String[] args)
            throws Exception
    {
        ParameterTool params = ParameterTool.fromArgs(args);

        int numPages = params.getInt("numPages", PageRankData.getNumberOfPages());
        int maxIterations = params.getInt("iterations", 10);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataSet pagesInput = getPagesDataSet(env, params);
        DataSet linksInput = getLinksDataSet(env, params);

        DataSet pagesWithRanks = pagesInput
                .map(new RankAssigner(1.0D / numPages));

        DataSet adjacencyListInput = linksInput
                .groupBy(new int[] { 0 })
                .reduceGroup(new BuildOutgoingEdgeList());

        IterativeDataSet iteration = pagesWithRanks.iterate(maxIterations);

        DataSet newRanks = iteration
                .join(adjacencyListInput)
                .where(new int[] { 0 }).equalTo(new int[] { 0 }).flatMap(new JoinVertexWithEdgesMatch())
                .groupBy(new int[] { 0 })
                .aggregate(Aggregations.SUM, 1)
                .map(new Dampener(0.85D, numPages));

        DataSet finalPageRanks = iteration.closeWith(newRanks, newRanks
                .join(iteration)
                .where(new int[] { 0 }).equalTo(new int[] { 0 })
                .filter(new EpsilonFilter()));

        if (params.has("output")) {
            finalPageRanks.writeAsCsv(params.get("output"), "\n", " ");

            env.execute("Basic Page Rank Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            finalPageRanks.print();
        }
    }

    private static DataSet<Long> getPagesDataSet(ExecutionEnvironment env, ParameterTool params)
    {
        MapFunction<Tuple1<Long>,Long> mapFunction = new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> v) throws Exception {
                return v.f0;
            }
        };
        if (params.has("pages"))
            return env.readCsvFile(params.get("pages"))
                    .fieldDelimiter(" ")
                    .lineDelimiter("\n")
                    .types(Long.class)
                    .map(mapFunction);
        System.out.println("Executing PageRank example with default pages data set.");
        System.out.println("Use --pages to specify file input.");
        return PageRankData.getDefaultPagesDataSet(env);
    }

    private static DataSet<Tuple2<Long, Long>> getLinksDataSet(ExecutionEnvironment env, ParameterTool params)
    {
        if (params.has("links")) {
            return env.readCsvFile(params.get("links"))
                    .fieldDelimiter(" ")
                    .lineDelimiter("\n")
                    .types(Long.class, Long.class);
        }

        System.out.println("Executing PageRank example with default links data set.");
        System.out.println("Use --links to specify file input.");
        return PageRankData.getDefaultEdgeDataSet(env);
    }

    public static final class EpsilonFilter
            implements FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>>
    {
        public boolean filter(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value)
        {
            return Math.abs(((Double)((Tuple2)value.f0).f1).doubleValue() - ((Double)((Tuple2)value.f1).f1).doubleValue()) > 0.0001D;
        }
    }

    @FunctionAnnotation.ForwardedFields({"0"})
    public static final class Dampener
            implements MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>
    {
        private final double dampening;
        private final double randomJump;

        public Dampener(double dampening, double numVertices)
        {
            this.dampening = dampening;
            this.randomJump = ((1.0D - dampening) / numVertices);
        }

        public Tuple2<Long, Double> map(Tuple2<Long, Double> value)
        {
            value.f1 = Double.valueOf(((Double)value.f1).doubleValue() * this.dampening + this.randomJump);
            return value;
        }
    }

    public static final class JoinVertexWithEdgesMatch
            implements FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple2<Long, Double>>
    {
        public void flatMap(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value, Collector<Tuple2<Long, Double>> out)
        {
            Long[] neighbors = (Long[])((Tuple2)value.f1).f1;
            double rank = ((Double)((Tuple2)value.f0).f1).doubleValue();
            double rankToDistribute = rank / neighbors.length;

            for (Long neighbor : neighbors)
                out.collect(new Tuple2(neighbor, Double.valueOf(rankToDistribute)));
        }
    }

    @FunctionAnnotation.ForwardedFields({"0"})
    public static final class BuildOutgoingEdgeList
            implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>>
    {
        private final ArrayList<Long> neighbors = new ArrayList();

        public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long[]>> out)
        {
            this.neighbors.clear();
            Long id = Long.valueOf(0L);

            for (Tuple2 n : values) {
                id = (Long)n.f0;
                this.neighbors.add((Long) n.f1);
            }
            out.collect(new Tuple2(id, this.neighbors.toArray(new Long[this.neighbors.size()])));
        }
    }

    public static final class RankAssigner
            implements MapFunction<Long, Tuple2<Long, Double>>
    {
        Tuple2<Long, Double> outPageWithRank;

        public RankAssigner(double rank)
        {
            this.outPageWithRank = new Tuple2(Long.valueOf(-1L), Double.valueOf(rank));
        }

        public Tuple2<Long, Double> map(Long page)
        {
            this.outPageWithRank.f0 = page;
            return this.outPageWithRank;
        }
    }
}
