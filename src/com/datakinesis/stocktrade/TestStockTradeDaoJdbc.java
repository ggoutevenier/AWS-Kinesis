package com.datakinesis.stocktrade;

import java.sql.Connection;
import java.sql.DriverManager;

import com.datakinesis.ObjectDao;
import com.datakinesis.Serde;
import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade;
import com.amazonaws.services.kinesis.samples.stocktrades.writer.StockTradeGenerator;
/**
 * Test program to test classes 
 * 		StockTradeGenerator 
 *  	SockTradeSerde
 *  	StockTableDaoJdbc
 *  all are working correctly before putting it through a kinesis stream 
 */
public class TestStockTradeDaoJdbc extends Parameters {
	static public void main(String[] args) {
		StockTradeGenerator generator = null;

		try {
			// create Dao object
			Class.forName(driverName);
			Connection connection = DriverManager.getConnection(URL, usr, pwd);
			connection.setAutoCommit(false); 
			ObjectDao objectDao = new StockTradeDaoJdbc(connection);
			
			Serde serde = new StockTradeSerde();					// create serde object for stock trades		
			
			generator = new StockTradeGenerator();

			for(int i=0;i<100;i++) {								// generate 100 random trades an put into database after serde
				StockTrade stockTrade1, stockTrade2;
				stockTrade1 = generator.getRandomTrade(); 			// generate random stock trade
				byte[] bytes=serde.serialize(stockTrade1); 			// serialize the stock trade into bytes
				stockTrade2=(StockTrade) serde.deserialize(bytes);	// deserialize the bytes of data back into a new stocktrade
				objectDao.add(stockTrade2);							// write the stocktrade to the database
			}
			objectDao.flush();										// flush/commit transations to the database
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("done");
	}
}
