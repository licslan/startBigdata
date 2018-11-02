package flinkplay;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class PageRankData
{
    public static final Object[][] EDGES = { {
            Long.valueOf(1L),
            Long.valueOf(2L) }, {
            Long.valueOf(1L),
            Long.valueOf(15L) }, {
            Long.valueOf(2L),
            Long.valueOf(3L) }, {
            Long.valueOf(2L),
            Long.valueOf(4L) }, {
            Long.valueOf(2L),
            Long.valueOf(5L) }, {
            Long.valueOf(2L),
            Long.valueOf(6L) }, {
            Long.valueOf(2L),
            Long.valueOf(7L) }, {
            Long.valueOf(3L),
            Long.valueOf(13L) }, {
            Long.valueOf(4L),
            Long.valueOf(2L) }, {
            Long.valueOf(5L),
            Long.valueOf(11L) }, {
            Long.valueOf(5L),
            Long.valueOf(12L) }, {
            Long.valueOf(6L),
            Long.valueOf(1L) }, {
            Long.valueOf(6L),
            Long.valueOf(7L) }, {
            Long.valueOf(6L),
            Long.valueOf(8L) }, {
            Long.valueOf(7L),
            Long.valueOf(1L) }, {
            Long.valueOf(7L),
            Long.valueOf(8L) }, {
            Long.valueOf(8L),
            Long.valueOf(1L) }, {
            Long.valueOf(8L),
            Long.valueOf(9L) }, {
            Long.valueOf(8L),
            Long.valueOf(10L) }, {
            Long.valueOf(9L),
            Long.valueOf(14L) }, {
            Long.valueOf(9L),
            Long.valueOf(1L) }, {
            Long.valueOf(10L),
            Long.valueOf(1L) }, {
            Long.valueOf(10L),
            Long.valueOf(13L) }, {
            Long.valueOf(11L),
            Long.valueOf(12L) }, {
            Long.valueOf(11L),
            Long.valueOf(1L) }, {
            Long.valueOf(12L),
            Long.valueOf(1L) }, {
            Long.valueOf(13L),
            Long.valueOf(14L) }, {
            Long.valueOf(14L),
            Long.valueOf(12L) }, {
            Long.valueOf(15L),
            Long.valueOf(1L) } };

    private static int numPages = 15;

    public static DataSet<Tuple2<Long, Long>> getDefaultEdgeDataSet(ExecutionEnvironment env)
    {
        List edges = new ArrayList();
        for (Object[] e : EDGES) {
            edges.add(new Tuple2((Long)e[0], (Long)e[1]));
        }
        return env.fromCollection(edges);
    }

    public static DataSet<Long> getDefaultPagesDataSet(ExecutionEnvironment env) {
        return env.generateSequence(1L, 15L);
    }

    public static int getNumberOfPages() {
        return numPages;
    }
}