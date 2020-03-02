package com.ajbose.upgrad;

import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class MovingAverageFunctions {

    static Function2<Tuple2<StockData, Integer>, Tuple2<StockData, Integer>, Tuple2<StockData, Integer>> movingAverageReduceFunc = new Function2<Tuple2<StockData, Integer>, Tuple2<StockData, Integer>,
            Tuple2<StockData, Integer>>() {
        @Override
        public Tuple2<StockData, Integer> call(Tuple2<StockData, Integer> valuePresent, Tuple2<StockData, Integer> valueIncoming) throws
                Exception {
            int outputCount = valuePresent._2 + valueIncoming._2;
            float aggregatedClosePrice = valuePresent._1.priceData.close + valueIncoming._1.priceData.close;
            PriceData newpriceData = new PriceData();
            newpriceData.setClose(aggregatedClosePrice);
            StockData newStockData = new StockData();
            newStockData.setPriceData(newpriceData);
            System.out.println("movingAverageReduceFunc, valuePresent: { symbol:" + valuePresent._1.symbol +",price: "+valuePresent._1.priceData.close+",count: "+ valuePresent._2+"}");
            System.out.println("movingAverageReduceFunc, valueOutgoing: { symbol:" + valueIncoming._1.symbol +",price: "+valueIncoming._1.priceData.close+",count: "+ valueIncoming._2+"}");
            System.out.println("movingAverageReduceFunc, Result: { symbol:" + valueIncoming._1.symbol +",price: "+newpriceData.close+",count: "+ outputCount+"}");
            return new Tuple2<StockData, Integer>(newStockData, outputCount);
        }
    };

    static Function2<Tuple2<StockData, Integer>, Tuple2<StockData, Integer>, Tuple2<StockData, Integer>> movingAverageInverseReduceFunc = new Function2<Tuple2<StockData, Integer>, Tuple2<StockData, Integer>,
            Tuple2<StockData, Integer>>() {
        @Override
        public Tuple2<StockData, Integer> call(Tuple2<StockData, Integer> valuePresent, Tuple2<StockData, Integer> valueOutgoing) throws
                Exception {
            int outputCount = valuePresent._2 - valueOutgoing._2;
            float aggregatedClosePrice = valuePresent._1.priceData.close - valueOutgoing._1.priceData.close;
            PriceData newpriceData = new PriceData();
            newpriceData.setClose(aggregatedClosePrice);
            StockData newStockData = new StockData();
            newStockData.setPriceData(newpriceData);
            System.out.println("movingAverageInverseReduceFunc, valuePresent: { symbol:" + valuePresent._1.symbol +",price: "+valuePresent._1.priceData.close+",count: "+ valuePresent._2+"}");
            System.out.println("movingAverageInverseReduceFunc, valueOutgoing: { symbol:" + valueOutgoing._1.symbol +",price: "+valueOutgoing._1.priceData.close+",count: "+ valueOutgoing._2+"}");
            System.out.println("movingAverageInverseReduceFunc, Result: { symbol:" + valueOutgoing._1.symbol +",price: "+newpriceData.close+",count: "+ outputCount+"}");
            return new Tuple2<StockData, Integer>(newStockData, outputCount);
        }
    };

}
