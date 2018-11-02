package com.licslan.mlplay;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
/**
 * @author  Weilin Huang
 * 随机森林 demo
 *
 * 提交方式：
 *
 * spark-submit \
 *         --master spark://服务器ip:7077 \  //no hard code in your code
 *         --class com.icslan.mlplay.MlMain \
 *         --executor-memory 4g \
 *         --executor-cores 4 \
 *         /linux path of jars/scala-1.0-SNAPSHOT.jar
 *
 * */
public class MlMain {

    /**
     * MLlib Operations
     * You can also easily use machine learning algorithms provided by MLlib. First of all, there are streaming machine learning algorithms
     * (e.g. Streaming Linear Regression, Streaming KMeans, etc.) which can simultaneously learn from the streaming data as well as apply
     * the model on the streaming data. Beyond these, for a much larger class of machine learning algorithms, you can learn a learning model offline
     * (i.e. using historical data) and then apply the model online on streaming data. See the MLlib guide for more details.
     */

    public static void main(String[] args) {

//        SparkConf conf = new SparkConf()
//                 .setMaster("local[*]")
//                .setAppName("StreamingKafka");
//        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(2));

        SparkSession spark = SparkSession
                .builder()
                .config("spark.some.config.option", "some-value")
                .appName("Spark ML Example")
                /*.master("spark://服务器ip:7077")*/
                .getOrCreate();

        // Load and parse the data file, converting it to a DataFrame.
        Dataset<Row> data = spark.read().format("libsvm").load("dataPool/data/mllib/sample_libsvm_data.txt");

        // Index labels, adding metadata to the label column.
        // Fit on whole dataset to include all labels in index.
        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexedLabel")
                .fit(data);
        // Automatically identify categorical features, and index them.
        // Set maxCategories so features with > 4 distinct values are treated as continuous.
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(4)
                .fit(data);

        // Split the data into training and test sets (30% held out for testing)
        Dataset<Row>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        // Train a RandomForest model.
        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures");

        // Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

        // Chain indexers and forest in a Pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{labelIndexer, featureIndexer, rf, labelConverter});

        // Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(trainingData);

        // Make predictions.
        Dataset<Row> predictions = model.transform(testData);

        // Select example rows to display.
        predictions.select("predictedLabel", "label", "features").show(5);

        // Select (prediction, true label) and compute test error
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test Error = " + (1.0 - accuracy));

        RandomForestClassificationModel rfModel = (RandomForestClassificationModel) (model.stages()[2]);
        System.out.println("Learned classification forest model:\n" + rfModel.toDebugString());

        spark.stop();
    }
}
