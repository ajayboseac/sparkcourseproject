package com.ajbose.upgrad;

import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class MaxProfitFunctions {

    static Function2<Tuple2<StockData, Integer>, Tuple2<StockData, Integer>, Tuple2<StockData, Integer>> maxProfitReduceFunc = new Function2<Tuple2<StockData, Integer>, Tuple2<StockData, Integer>,
            Tuple2<StockData, Integer>>() {
        @Override
        public Tuple2<StockData, Integer> call(Tuple2<StockData, Integer> valuePresent, Tuple2<StockData, Integer> valueIncoming) throws
                Exception {
            int outputCount = valuePresent._2 + valueIncoming._2;
            float openingPriceSum = valuePresent._1.priceData.open + valueIncoming._1.priceData.open;
            float closingPriceSum = valuePresent._1.priceData.close + valueIncoming._1.priceData.close;
            PriceData newpriceData = new PriceData();
            newpriceData.setClose(closingPriceSum);
            newpriceData.setOpen(openingPriceSum);
            StockData newStockData = new StockData();
            newStockData.setPriceData(newpriceData);
            System.out.println("maxProfitReduceFunc, valuePresent: { symbol:" + valuePresent._1.symbol + ",openPrice: " + valuePresent._1.priceData.open + ",closePrice: " + valuePresent._1.priceData.close + ",count: " + valuePresent._2 + "}");
            System.out.println("maxProfitReduceFunc, valueOutgoing: { symbol:" + valueIncoming._1.symbol + ",openPrice: " + valueIncoming._1.priceData.open + ",closePrice: " + valueIncoming._1.priceData.close + ",count: " + valueIncoming._2 + "}");
            System.out.println("maxProfitReduceFunc, valueOutgoing: { symbol:" + valueIncoming._1.symbol + ",openPrice: " + newStockData.priceData.open + ",closePrice: " + newStockData.priceData.close + ",count: " + outputCount + "}");
            return new Tuple2<StockData, Integer>(newStockData, outputCount);
        }
    };

    static Function2<Tuple2<StockData, Integer>, Tuple2<StockData, Integer>, Tuple2<StockData, Integer>> maxProfitInverseReduceFunc = new Function2<Tuple2<StockData, Integer>, Tuple2<StockData, Integer>,
            Tuple2<StockData, Integer>>() {
        @Override
        public Tuple2<StockData, Integer> call(Tuple2<StockData, Integer> valuePresent, Tuple2<StockData, Integer> valueOutgoing) throws
                Exception {
            int outputCount = valuePresent._2 - valueOutgoing._2;
            float openingPriceSum = valuePresent._1.priceData.open - valueOutgoing._1.priceData.open;
            float closingPriceSum = valuePresent._1.priceData.close - valueOutgoing._1.priceData.close;
            PriceData newpriceData = new PriceData();
            newpriceData.setClose(closingPriceSum);
            newpriceData.setOpen(openingPriceSum);
            StockData newStockData = new StockData();
            newStockData.setPriceData(newpriceData);
            System.out.println("maxProfitInverseReduceFunc, valuePresent: { symbol:" + valuePresent._1.symbol + ",openPrice: " + valuePresent._1.priceData.open + ",closePrice: " + valuePresent._1.priceData.close + ",count: " + valuePresent._2 + "}");
            System.out.println("maxProfitInverseReduceFunc, valueOutgoing: { symbol:" + valueOutgoing._1.symbol + ",openPrice: " + valueOutgoing._1.priceData.open + ",closePrice: " + valueOutgoing._1.priceData.close + ",count: " + valueOutgoing._2 + "}");
            System.out.println("maxProfitInverseReduceFunc, valueOutgoing: { symbol:" + valueOutgoing._1.symbol + ",openPrice: " + newStockData.priceData.open + ",closePrice: " + newStockData.priceData.close + ",count: " + outputCount + "}");
            return new Tuple2<StockData, Integer>(newStockData, outputCount);
        }
    };


}
