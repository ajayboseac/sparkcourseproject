package com.ajbose.upgrad;

import java.io.Serializable;

public class StockData implements Serializable {
    String symbol;
    String timestamp;
    PriceData priceData;

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timeStamp) {
        this.timestamp = timeStamp;
    }

    public PriceData getPriceData() {
        return priceData;
    }

    public void setPriceData(PriceData priceData) {
        this.priceData = priceData;
    }

    @Override
    public String toString() {
        return "StockData{" +
                "symbol='" + symbol + '\'' +
                ", timeStamp='" + timestamp + '\'' +
                ", priceData=" + priceData +
                '}';
    }
}
