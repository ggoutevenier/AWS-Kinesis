package com.datakinesis.stocktrade;
import java.io.IOException;

import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade;
import com.datakinesis.Serde;
import com.fasterxml.jackson.databind.ObjectMapper;


public class StockTradeSerde implements Serde{
    private final static ObjectMapper JSON = new ObjectMapper();
	@Override
	public byte[] serialize(Object object) {
        try {
            return JSON.writeValueAsBytes((StockTrade)object);
        } catch (IOException e) {
            return null;
        }
	}

	@Override
	public Object deserialize(byte[] bytes) {
        try {
            return JSON.readValue(bytes, StockTrade.class);
        } catch (IOException e) {
            return null;
        }
	}

}
