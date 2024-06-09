package RDDExternalDataset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class RDDExternalDatasetTest {


    private SparkConf sparkConf = new SparkConf().setAppName("RDDExternalDatasetTest")
            .setMaster("local[*]");


    @ParameterizedTest
    @ValueSource(strings= {"src/main/resources/names.txt","src/main/resources/weirednames.txt","src/main/resources/cities.txt"})
    void TestMe(final String filePath) {
        try(JavaSparkContext jsc = new JavaSparkContext(sparkConf)){
            jsc.textFile(filePath).take(5).forEach(System.out::println);
            System.out.println("######## ");
        }


    }
}
