package com.epam.data_frames;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;


public class Main {

    public static final String KEYWORDS = "keywords";
    public static final String SALARY = "salary";
    public static final String KEYWORD = "keyword";

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        System.setProperty("hadoop.home.dir", "C:\\winutils");

        SparkConf sparkConf = new SparkConf().setAppName("linkedIn").setMaster("local[*]").set("spark.testing.memory", "512000000");
        SparkContext sc = new SparkContext(sparkConf);
        SQLContext spark = new SQLContext(sc);

        //1. create and print the data
        DataFrame dataFrame = spark.read().json("data/linkedIn/*");
        System.out.println("\nPrint out file content:");
        dataFrame.show();

        //2. print the schema
        System.out.println("Print out the schema");
        dataFrame.printSchema();

        //3. print column type
        System.out.println("Print out the column type");
        Arrays.stream(dataFrame.schema().fields()).forEach(x-> System.out.println(x.name() +" :"+ x.dataType()));

        //4. add salary
        DataFrame dataFrameWithSalary = dataFrame.withColumn(SALARY, col("age")
                .cast(DataTypes.IntegerType)
                .multiply(10)
                .multiply(size(col(KEYWORDS)))
        );
        System.out.println("Print data with salaries:");
        dataFrameWithSalary.show();

        //5. salary < 1200 and familiar with most popular technology
        System.out.println("Print the people with salary < 1200 which familiar the most popular keyword:");

        String mostPopular = dataFrame.withColumn(KEYWORD, explode(col(KEYWORDS)))
                .select(col(KEYWORD))
                .toJavaRDD()
                .mapToPair(word -> new Tuple2<>(word.mkString(), 1))
                .reduceByKey(Integer::sum)
                .mapToPair(word -> new Tuple2<>(word._2, word._1))
                .sortByKey(false)
                .map(Tuple2::swap)
                .take(1)
                .get(0)._1;

        System.out.println("Most popular keyword is " + mostPopular);
        dataFrameWithSalary.where(array_contains(col(KEYWORDS), mostPopular)).where(col(SALARY).$less(1200)).show();
    }
}
