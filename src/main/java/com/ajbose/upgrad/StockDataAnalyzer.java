package com.ajbose.upgrad;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

import static com.ajbose.upgrad.MaxProfitFunctions.maxProfitInverseReduceFunc;
import static com.ajbose.upgrad.MaxProfitFunctions.maxProfitReduceFunc;
import static com.ajbose.upgrad.MovingAverageFunctions.movingAverageInverseReduceFunc;
import static com.ajbose.upgrad.MovingAverageFunctions.movingAverageReduceFunc;

public class StockDataAnalyzer {


    static PairFunction<String, String, Tuple2<StockData, Integer>> pairFunctionWithCount = new PairFunction<String, String, Tuple2<StockData, Integer>>() {
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

    static PairFunction<String, String,StockData> pairFunction = new PairFunction<String, String, StockData>() {
        private static final long serialVersionUID = 1L;

        public Tuple2<String, StockData> call(String x) throws Exception {

            //creating mapper object
            ObjectMapper mapper = new ObjectMapper();

            // defining the return type
            TypeReference<StockData> mapType = new TypeReference<StockData>() {
            };

            // Parsing the JSON String
            StockData stockData = mapper.readValue(x, mapType);

            return new Tuple2<String, StockData>(stockData.getSymbol(), stockData);

        }
    };

    public static void main(String[] args) throws InterruptedException {
        Map<String, Object> kafkaParams = getKafkaParams();
        Collection<String> topics = Arrays.asList("stockData");
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("StockDataAnalyzer");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.minutes(1l));
        Logger.getRootLogger().setLevel(Level.WARN);


        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        javaStreamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        stream.mapToPair(it -> pairFunctionWithCount.call(it.value())).map(it -> {
            System.out.println("Incoming batch:");
            return it;
        }).print();

        stream.mapToPair(it -> pairFunctionWithCount.call(it.value()))
                .reduceByKeyAndWindow(movingAverageReduceFunc, movingAverageInverseReduceFunc, Durations.minutes(10), Durations.minutes(5))
                .mapValues(it -> it._1.priceData.close / it._2)
                .print();

        stream.mapToPair(it -> pairFunctionWithCount.call(it.value()))
                .reduceByKeyAndWindow(maxProfitReduceFunc, maxProfitInverseReduceFunc, Durations.minutes(10), Durations.minutes(5))
                .mapValues(it -> it._1.priceData.close / it._2-it._1.priceData.open / it._2 )
                .reduce((x, y) -> {
                    if (x._2 > y._2) {
                        return x;
                    } else {
                        return y;
                    }
                })
                .print();

        stream.mapToPair(it -> pairFunction.call(it.value())).reduceByKeyAndWindow(
                (stockdat1,stockData2)->{
                    StockData newStockData = new StockData();
                    retu
                }
        )

        javaStreamingContext.checkpoint("resources/checkpoint");
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
