package com.ajbose.upgrad;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class StockDataAnalyzer {

    public static void main(String[] args) throws InterruptedException {
        Map<String, Object> kafkaParams = getKafkaParams();
        Collection<String> topics = Arrays.asList("stockData");
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("StockDataAnalyzer");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.minutes(1l));
        Logger.getRootLogger().setLevel(Level.WARN);


        PairFunction<String, String, Tuple2<StockData, Integer>> pairFunction = new PairFunction<String, String, Tuple2<StockData, Integer>>() {
            private static final long serialVersionUID = 1L;

            public Tuple2<String, Tuple2<StockData, Integer>> call(String x) throws Exception {

                //creating mapper object
                ObjectMapper mapper = new ObjectMapper();

                // defining the return type
                TypeReference<StockData> mapType = new TypeReference<StockData>() {
                };

                // Parsing the JSON String
                StockData stockData = mapper.readValue(x, mapType);

                return new Tuple2<String, Tuple2<StockData, Integer>>(stockData.getSymbol(), new Tuple2<StockData, Integer>(stockData, 1));

            }
        };


        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        javaStreamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        stream.mapToPair(it -> pairFunction.call(it.value())).map(it -> {
            System.out.println("Incoming batch:");
            return it;
        }).print();


        stream.mapToPair(it -> pairFunction.call(it.value()))
                .window(Durations.minutes(10), Durations.minutes(5))
                .reduceByKey((Tuple2<StockData, Integer> x, Tuple2<StockData, Integer> y) -> {
                    int outputY = x._2 + y._2;
                    float aggregatedClosePrice = x._1.priceData.close + y._1.priceData.close;
                    PriceData newpriceData = new PriceData();
                    newpriceData.setClose(aggregatedClosePrice);
                    StockData newStockData = new StockData();
                    newStockData.setPriceData(newpriceData);
                    return new Tuple2<StockData, Integer>(newStockData, outputY);
                })
                .mapValues(it -> it._1.priceData.close / it._2)
                .map(it -> { return it; })
                .transform(rdd -> { System.out.println("Moving average of close price: ");return rdd; })
                .print();


        stream.mapToPair(it -> pairFunction.call(it.value()))
                .window(Durations.minutes(10), Durations.minutes(5))
                .reduceByKey((Tuple2<StockData, Integer> x, Tuple2<StockData, Integer> y) -> {
                    int outputY = x._2 + y._2;
                    float openingPriceSum = x._1.priceData.open + y._1.priceData.open;
                    float closingPriceSum = x._1.priceData.close + y._1.priceData.close;
                    PriceData newpriceData = new PriceData();
                    newpriceData.setClose(closingPriceSum);
                    newpriceData.setOpen(openingPriceSum);
                    StockData newStockData = new StockData();
                    newStockData.setPriceData(newpriceData);
                    return new Tuple2<StockData, Integer>(newStockData, outputY);
                }).mapValues(it -> {
            return it._1.priceData.open / it._2 - it._1.priceData.close / it._2;
        })
                .reduce((x, y) -> {
                    if (x._2 > y._2) {
                        return x;
                    } else {
                        return y;
                    }
                })
                .transform(rdd -> { System.out.println("Stock giving maximum profit: ");return rdd; })
                .print();


//        sortedData.transformToPair(rdd->rdd.filter(rdd.take(1).toArray().));
//                transform(rdd -> {
//                rdd.filter(rdd.take(n).toList.contains)
//        })

//        .mapValues((Tuple2<StockData, Integer> it )-> it._1.priceData.averagePrice / it._2)


        javaStreamingContext.start();

        javaStreamingContext.awaitTermination();
        javaStreamingContext.close();

    }

    private static Map<String, Object> getKafkaParams() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "52.55.237.11:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", UUID.randomUUID().toString());
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        return kafkaParams;
    }

}
