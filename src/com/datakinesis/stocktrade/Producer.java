package com.datakinesis.stocktrade;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.samples.stocktrades.writer.StockTradeGenerator;
import com.datakinesis.KinesisProducer;
import com.datakinesis.Serde;

public class Producer {
	public static void main(String[] args) {
		String streamName = "stocktrade_stream";
		String appName = "stocktrade_app";
		String appVersion = "1.0.0";
		String regionName = "us-west-2";
		Serde serde = new StockTradeSerde();
		KinesisProducer kinesisProducer=null;
		int numTrades = 0;

		if (args.length != 1) {
			System.out.println("Usage: NumTrades");
			System.exit(1);
		}
		try {
			numTrades = Integer.parseInt(args[0]);
			ProfileCredentialsProvider credentials = new ProfileCredentialsProvider();

			kinesisProducer = new KinesisProducer(credentials, streamName, appName, appVersion, regionName, serde);
			StockTradeGenerator generator = new StockTradeGenerator();

			for (int i = 0; i < numTrades; i++) {
				kinesisProducer.add(generator.getRandomTrade());
			}
			kinesisProducer.close();
		} catch (Exception e) {
			System.out.println("Exception: " + e.getMessage());
		}
		
		System.out.println("Done Transactions: " + numTrades);
	}
}
