package com.datakinesis.stocktrade;

import java.sql.Connection;
import java.sql.DriverManager;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.datakinesis.ObjectDao;
import com.datakinesis.Serde;
import com.datakinesis.KinesisConsumer;

public class Consumer {

	public static void main(String[] args) {
		try {
			ProfileCredentialsProvider credentials = new ProfileCredentialsProvider();
			String streamName = "stocktrade_stream";
			String appName = "stocktrade_app";
			String appVersion = "1.0.0";
			String regionName = "us-west-2";
			String driverName = "com.mysql.jdbc.Driver";
			String URL = "jdbc:mysql://52.11.70.174:3306/stocktrade";
			String usr = "aperio";
			String pwd = "aperio9";

			Class.forName(driverName);
			Connection connection = DriverManager.getConnection(URL, usr, pwd);
			connection.setAutoCommit(false);
			ObjectDao objectDao = new StockTradeDaoJdbc(connection);
			Serde serde = new StockTradeSerde();

			KinesisConsumer consumer = new KinesisConsumer(
					credentials, 
					streamName, 
					appName, 
					appVersion, 
					regionName,
					serde,
					objectDao
					);

			consumer.run();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			System.exit(1);
		}
	}
}