package com.licslan.sqlplay;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;

// col("...") is preferable to df.col("...")
/**
 * @author  Weilin Huang
 * spark sql demo
 *
 * 提交方式：
 *
 * spark-submit \
 *         --master local[*] \  //no hard code in your code
 *         --class com.icslan.sqlplay.SqlMain \
 *         --executor-memory 4g \
 *         --executor-cores 4 \
 *         /linux path of jars/scala-1.0-SNAPSHOT.jar
 *
 * */
public class SqlMain {

    public static void main(String[] args) throws Exception{

        /** 创建spark sql环境*/
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                /*.config("spark.some.config.option", "some-value")*/
                .appName("spark sql")
                .getOrCreate();

        /** spark sql  数据来源
         *
         * 1.连接关系型数据库  jdbc
         * 2.本地文件/redis/mongodb/json data/...转换为可以sql语句操作的数据
         * */
        Dataset<Row> df = spark.read().json("D:\\PROJECT-HWLING\\Now-ing\\scala_spark_flink_study\\startBigData\\src\\main\\dataPool\\people.json");

        // Displays the content of the DataFrame to stdout
        df.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+


        /**
         * Untyped Dataset Operations (aka DataFrame Operations)
         * DataFrames provide a domain-specific language for structured data manipulation in Scala, Java, Python and R.
         *
         * As mentioned above, in Spark 2.0, DataFrames are just Dataset of Rows in Scala and Java API. These operations
         * are also referred as “untyped transformations” in contrast to “typed transformations” come with strongly typed Scala/Java Datasets.
         *
         * Here we include some basic examples of structured data processing using Datasets:
         * */

        // Print the schema in a tree format
        df.printSchema();
        // root
        // |-- age: long (nullable = true)
        // |-- name: string (nullable = true)

        // Select only the "name" column
        df.select("name").show();
        // +-------+
        // |   name|
        // +-------+
        // |Michael|
        // |   Andy|
        // | Justin|
        // +-------+

        // Select everybody, but increment the age by 1
        df.select(col("name"), col("age").plus(1)).show();
        // +-------+---------+
        // |   name|(age + 1)|
        // +-------+---------+
        // |Michael|     null|
        // |   Andy|       31|
        // | Justin|       20|
        // +-------+---------+

        // Select people older than 21
        df.filter(col("age").gt(21)).show();
        // +---+----+
        // |age|name|
        // +---+----+
        // | 30|Andy|
        // +---+----+

        // Count people by age
        df.groupBy("age").count().show();
        // +----+-----+
        // | age|count|
        // +----+-----+
        // |  19|    1|
        // |null|    1|
        // |  30|    1|
        // +----+-----+


        /**
         * Find full example code at "examples/src/main/java/org/apache/spark/examples/sql/JavaSparkSQLExample.java" in the Spark repo.
         * For a complete list of the types of operations that can be performed on a Dataset refer to the API Documentation.
         *
         * In addition to simple column references and expressions, Datasets also have a rich library of functions including string
         * manipulation, date arithmetic, common math operations and more. The complete list is available in the DataFrame Function Reference.
         * */


        //Running SQL Queries Programmatically

        //The sql function on a SparkSession enables applications to run SQL queries programmatically and returns the result as a Dataset<Row>.


        //将之前的数据作为一张表创建
        df.createOrReplaceTempView("licslan");
        Dataset<Row> sqlDF = spark.sql("SELECT name FROM licslan");
        sqlDF.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+

        /**
         * Global Temporary View
         * Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates.
         * If you want to have a temporary view that is shared among all sessions and keep alive until the Spark application
         * terminates, you can create a global temporary view. Global temporary view is tied to a system preserved database
         * global_temp, and we must use the qualified name to refer it, e.g. SELECT * FROM global_temp.view1.
          */

        // Register the DataFrame as a global temporary view
        df.createGlobalTempView("people");

        // Global temporary view is tied to a system preserved database `global_temp`
        spark.sql("SELECT * FROM global_temp.people").show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+

        // Global temporary view is cross-session
        spark.newSession().sql("SELECT * FROM global_temp.people").show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+



        /**
         * Creating Datasets
         * Datasets are similar to RDDs, however, instead of using Java serialization or Kryo
         * they use a specialized Encoder to serialize the objects for processing or transmitting over the network.
         * While both encoders and standard serialization are responsible for turning an object into bytes, encoders are code generated dynamically
         * and use a format that allows Spark to perform many operations like filtering, sorting and hashing without deserializing the bytes back into an object.
         * */




        // Create an instance of a Bean class
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);

        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();
        // +---+----+
        // |age|name|
        // +---+----+
        // | 32|Andy|
        // +---+----+

        // Encoders for most common types are provided in class Encoders
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map(
                (MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder);
        transformedDS.collect(); // Returns [2, 3, 4]

        // DataFrames can be converted to a Dataset by providing a class. Mapping based on name
        String path = "D:\\PROJECT-HWLING\\Now-ing\\scala_spark_flink_study\\startBigData\\src\\main\\dataPool\\people.json";
        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        peopleDS.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+





        /**
         * Interoperating with RDDs
         * Spark SQL supports two different methods for converting existing RDDs into Datasets. The first method uses reflection to infer the
         * schema of an RDD that contains specific types of objects. This reflection based approach leads to more concise code and works well
         * when you already know the schema while writing your Spark application.
         *
         * The second method for creating Datasets is through a programmatic interface that allows you to construct a schema and then apply
         * it to an existing RDD. While this method is more verbose, it allows you to construct Datasets when the columns and their types are not known until runtime.
         * */

        // Create an RDD of Person objects from a text file
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile("D:\\PROJECT-HWLING\\Now-ing\\scala_spark_flink_study\\startBigData\\src\\main\\dataPool\\people.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person persons = new Person();
                    persons.setName(parts[0]);
                    persons.setAge(Integer.parseInt(parts[1].trim()));
                    return persons;
                });

            // Apply a schema to an RDD of JavaBeans to get a DataFrame
            Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
            // Register the DataFrame as a temporary view
            peopleDF.createOrReplaceTempView("people");

            // SQL statements can be run by using the sql methods provided by spark
            Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

            // The columns of a row in the result can be accessed by field index
            Encoder<String> stringEncoder = Encoders.STRING();
            Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                    (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                    stringEncoder);
            teenagerNamesByIndexDF.show();
            // +------------+
            // |       value|
            // +------------+
            // |Name: Justin|
            // +------------+

            // or by field name
            Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                    (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                    stringEncoder);
            teenagerNamesByFieldDF.show();
            // +------------+
            // |       value|
            // +------------+
            // |Name: Justin|
            // +------------+




        //Programmatically Specifying the Schema

        /**
         * When JavaBean classes cannot be defined ahead of time (for example, the structure of records is encoded in a string,
         * or a text dataset will be parsed and fields will be projected differently for different users), a Dataset<Row> can be created programmatically with three steps.
         *
         * Create an RDD of Rows from the original RDD;
         * Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
         * Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession
         * */


        // Create an RDD
        JavaRDD<String> peopleRDDs = spark.sparkContext()
                .textFile("D:\\PROJECT-HWLING\\Now-ing\\scala_spark_flink_study\\startBigData\\src\\main\\dataPool\\people.txt", 1)
                .toJavaRDD();

        // The schema is encoded in a string
        String schemaString = "name age";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleRDDs.map((Function<String, Row>) record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        // Apply the schema to the RDD
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        // Creates a temporary view using the DataFrame
        peopleDataFrame.createOrReplaceTempView("people");

        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results = spark.sql("SELECT name FROM people");

        // The results of SQL queries are DataFrames and support all the normal RDD operations
        // The columns of a row in the result can be accessed by field index or by field name
        Dataset<String> namesDS = results.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();
        // +-------------+
        // |        value|
        // +-------------+
        // |Name: Michael|
        // |   Name: Andy|
        // | Name: Justin|
        // +-------------+




        /**
         * Aggregations
         * The built-in DataFrames functions provide common aggregations such as count(), countDistinct(), avg(), max(), min(), etc.
         * While those functions are designed for DataFrames, Spark SQL also has type-safe versions for some of them in Scala and
         * Java to work with strongly typed Datasets. Moreover, users are not limited to the predefined aggregate functions and can create their own.
         *
         * Untyped User-Defined Aggregate Functions
         * Users have to extend the UserDefinedAggregateFunction abstract class to implement a custom untyped aggregate function.
         * For example, a user-defined average can look like:
         * */


        // Register the function to access it
        spark.udf().register("myAverage", new MyAverage());

        Dataset<Row> dfs = spark.read().json("D:\\PROJECT-HWLING\\Now-ing\\scala_spark_flink_study\\startBigData\\src\\main\\dataPool\\employees.json");
        dfs.createOrReplaceTempView("employees");
        dfs.show();
        // +-------+------+
        // |   name|salary|
        // +-------+------+
        // |Michael|  3000|
        // |   Andy|  4500|
        // | Justin|  3500|
        // |  Berta|  4000|
        // +-------+------+

        Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
        result.show();
        // +--------------+
        // |average_salary|
        // +--------------+
        // |        3750.0|
        // +--------------+




        /**
         * Type-Safe User-Defined Aggregate Functions
         * User-defined aggregations for strongly typed Datasets revolve around the Aggregator abstract class.
         * For example, a type-safe user-defined average can look like:
         * */


        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        String paths = "D:\\PROJECT-HWLING\\Now-ing\\scala_spark_flink_study\\startBigData\\src\\main\\dataPool\\employees.json";
        Dataset<Employee> ds = spark.read().json(paths).as(employeeEncoder);
        ds.show();
        // +-------+------+
        // |   name|salary|
        // +-------+------+
        // |Michael|  3000|
        // |   Andy|  4500|
        // | Justin|  3500|
        // |  Berta|  4000|
        // +-------+------+

        MyAverages myAverages = new MyAverages();
        // Convert the function to a `TypedColumn` and give it a name
        TypedColumn<Employee, Double> averageSalary = myAverages.toColumn().name("average_salary");
        Dataset<Double> resultss = ds.select(averageSalary);
        resultss.show();
        // +--------------+
        // |average_salary|
        // +--------------+
        // |        3750.0|
        // +--------------+



        /**
         * Data Sources
         * Spark SQL supports operating on a variety of data sources through the DataFrame interface. A DataFrame can be operated on using relational
         * transformations and can also be used to create a temporary view. Registering a DataFrame as a temporary view allows you to run SQL queries
         * over its data. This section describes the general methods for loading and saving data using the Spark Data Sources and then goes into specific
         * options that are available for the built-in data sources.
         *
         * Generic Load/Save Functions
         * In the simplest form, the default data source (parquet unless otherwise configured by spark.sql.sources.default) will be used for all operations.
         * */


        Dataset<Row> usersDF = spark.read().load("D:\\PROJECT-HWLING\\Now-ing\\scala_spark_flink_study\\startBigData\\src\\main\\dataPool\\users.parquet");
        usersDF.select("name", "favorite_color").write().save("D:\\PROJECT-HWLING\\Now-ing\\scala_spark_flink_study\\startBigData\\src\\main\\dataPool\\namesAndFavColors.parquet");


        /**
         * Manually Specifying Options
         * You can also manually specify the data source that will be used along with any extra options that you would like to pass to the data source. Data sources
         * are specified by their fully qualified name (i.e., org.apache.spark.sql.parquet), but for built-in sources you can also use their short names (json, parquet,
         * jdbc, orc, libsvm, csv, text). DataFrames loaded from any data source type can be converted into other types using this syntax.
         * */


        Dataset<Row> peopleDFs =
                spark.read().format("json").load("D:\\PROJECT-HWLING\\Now-ing\\scala_spark_flink_study\\startBigData\\src\\main\\dataPool\\people.json");
        peopleDFs.select("name", "age").write().format("parquet").save("D:\\PROJECT-HWLING\\Now-ing\\scala_spark_flink_study\\startBigData\\src\\main\\dataPool\\namesAndAges.parquet");


        /**
         * Run SQL on files directly
         * Instead of using read API to load a file into DataFrame and query it, you can also query that file directly with SQL.
         * */

        Dataset<Row> sqlDFss =
                spark.sql("SELECT * FROM parquet.`D:\\PROJECT-HWLING\\Now-ing\\scala_spark_flink_study\\startBigData\\src\\main\\dataPool\\users.parquet`");

        sqlDFss.select("name", "age").write().format("parquet").save("namesAndAges.parquet");



        //  TODO   例子暂写到在这里

        // TODO  去连接MySQL  mongodb  redis  hbase  ....















        spark.stop();

    }


    public static class Employee implements Serializable {
        private String name;
        private long salary;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getSalary() {
            return salary;
        }

        public void setSalary(long salary) {
            this.salary = salary;
        }
    }

    public static class Average implements Serializable  {
        private long sum;
        private long count;

        public Average(long sum, long count) {
            this.sum = sum;
            this.count = count;
        }

        public long getSum() {
            return sum;
        }

        public void setSum(long sum) {
            this.sum = sum;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }

    public static class MyAverages extends Aggregator<Employee, Average, Double> {
        // A zero value for this aggregation. Should satisfy the property that any b + zero = b
        public Average zero() {
            return new Average(0L, 0L);
        }
        // Combine two values to produce a new value. For performance, the function may modify `buffer`
        // and return it instead of constructing a new object
        public Average reduce(Average buffer, Employee employee) {
            long newSum = buffer.getSum() + employee.getSalary();
            long newCount = buffer.getCount() + 1;
            buffer.setSum(newSum);
            buffer.setCount(newCount);
            return buffer;
        }
        // Merge two intermediate values
        public Average merge(Average b1, Average b2) {
            long mergedSum = b1.getSum() + b2.getSum();
            long mergedCount = b1.getCount() + b2.getCount();
            b1.setSum(mergedSum);
            b1.setCount(mergedCount);
            return b1;
        }
        // Transform the output of the reduction
        public Double finish(Average reduction) {
            return ((double) reduction.getSum()) / reduction.getCount();
        }
        // Specifies the Encoder for the intermediate value type
        public Encoder<Average> bufferEncoder() {
            return Encoders.bean(Average.class);
        }
        // Specifies the Encoder for the final output value type
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }




    public static class MyAverage extends UserDefinedAggregateFunction {

        private StructType inputSchema;
        private StructType bufferSchema;

        public MyAverage() {
            List<StructField> inputFields = new ArrayList<>();
            inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
            inputSchema = DataTypes.createStructType(inputFields);

            List<StructField> bufferFields = new ArrayList<>();
            bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
            bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
            bufferSchema = DataTypes.createStructType(bufferFields);
        }
        // Data types of input arguments of this aggregate function
        public StructType inputSchema() {
            return inputSchema;
        }
        // Data types of values in the aggregation buffer
        public StructType bufferSchema() {
            return bufferSchema;
        }
        // The data type of the returned value
        public DataType dataType() {
            return DataTypes.DoubleType;
        }
        // Whether this function always returns the same output on the identical input
        public boolean deterministic() {
            return true;
        }
        // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
        // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
        // the opportunity to update its values. Note that arrays and maps inside the buffer are still
        // immutable.
        public void initialize(MutableAggregationBuffer buffer) {
            buffer.update(0, 0L);
            buffer.update(1, 0L);
        }
        // Updates the given aggregation buffer `buffer` with new input data from `input`
        public void update(MutableAggregationBuffer buffer, Row input) {
            if (!input.isNullAt(0)) {
                long updatedSum = buffer.getLong(0) + input.getLong(0);
                long updatedCount = buffer.getLong(1) + 1;
                buffer.update(0, updatedSum);
                buffer.update(1, updatedCount);
            }
        }
        // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            long mergedSum = buffer1.getLong(0) + buffer2.getLong(0);
            long mergedCount = buffer1.getLong(1) + buffer2.getLong(1);
            buffer1.update(0, mergedSum);
            buffer1.update(1, mergedCount);
        }
        // Calculates the final result
        public Double evaluate(Row buffer) {
            return ((double) buffer.getLong(0)) / buffer.getLong(1);
        }
    }






    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }



}
