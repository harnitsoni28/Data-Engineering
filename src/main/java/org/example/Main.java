package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class FindMin{
    public static int min(int x, int y){
        return x>=y?y:x;
    }
}

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");

        SparkSession ss = SparkSession.builder()
                .appName("SparkFirstProg")
                .master("local[*]")
                .getOrCreate();


        try (JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext())){

            List<Integer> data = Stream.iterate(1, n->n+1)
                    .limit(10)
                    .collect(Collectors.toList());

            data.add(5);
            data.add(5);

            data.forEach(System.out::println);

            JavaRDD<Integer> myRDD = jsc.parallelize(data);

            System.out.println(myRDD.count());
            System.out.println(myRDD.getNumPartitions());

            Integer max= myRDD.reduce((x,y)-> Integer.max(x,y));
           // Integer min= myRDD.reduce(Integer::min);
            Integer min= myRDD.reduce((x,y) -> FindMin.min(x,y));
            Integer sum= myRDD.reduce(Integer::sum);

//            Map<Integer,Integer> map = new HashMap<>();
//            myRDD.mapToPair(x-> new Tuple2<>(x,1)).countByKey().forEach((x,y)-> map.put(x, Math.toIntExact(y)));
//            System.out.println(map);

            System.out.println(max);
            System.out.println(min);
            System.out.println(sum);



//    To sleep the progm for 10 min for using SparkUI

//            try {
//                Thread.sleep(600000);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }





        }
    }
}