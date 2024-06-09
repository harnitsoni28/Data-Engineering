package CreateRDDusingParallelize;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CreateRDDusingParallelizeTest {

    private SparkConf sparkConf= new SparkConf().setAppName("CreateRDDusingParallelizeTest")
            .setMaster("local[*]");


    @Test
    void createAnEmptyRDDwithNoPartitionsInSpark() {

    try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            JavaRDD<Object> emptyRDD = jsc.emptyRDD();
            System.out.println(emptyRDD);
            System.out.println(emptyRDD.getNumPartitions());

        }
    }

    @Test
    void createAnEmptyRDDwithDefaultPartitionsInSpark() {

        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            JavaRDD<Object> emptyRDD = jsc.parallelize(Arrays.asList());
            System.out.println(emptyRDD);
            System.out.println(emptyRDD.getNumPartitions());

        }
    }

    @Test
    void createAnEmptyRDDusingParallelizeMethod() {

        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            List<Integer> data = Stream.iterate(1, n-> n+1)
                    .limit(8)
                    .collect(Collectors.toList());

            JavaRDD<Integer> myRDD = jsc.parallelize(data);

            System.out.println(myRDD);
            System.out.println(myRDD.getNumPartitions());
            System.out.println(myRDD.count());
            myRDD.collect().forEach(System.out::println);


            // reduce operation
            Integer max= myRDD.reduce(Integer::max);
            Integer min= myRDD.reduce(Integer::min);
            Integer sum= myRDD.reduce(Integer::sum);

            System.out.println(max +" "+ min +" " + sum);
        }
    }
}
