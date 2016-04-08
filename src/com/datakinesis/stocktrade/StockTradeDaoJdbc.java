package com.datakinesis.stocktrade;

import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import com.datakinesis.ObjectDao;
import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade;

public class StockTradeDaoJdbc implements ObjectDao {
	private static final Log LOG = LogFactory.getLog(StockTradeDaoJdbc.class);
	private boolean needsFlush;
	private Connection connection;
	private PreparedStatement pstmt;

	@Override
	public void add(Object object) {
		try {
			StockTrade stockTrade = (StockTrade) object;
			pstmt.setLong(1, stockTrade.getId());
			pstmt.setDouble(2, stockTrade.getPrice());
			pstmt.setLong(3, stockTrade.getQuantity());
			pstmt.setString(4, stockTrade.getTickerSymbol());
			pstmt.setString(5,stockTrade.getTradeType().name());
			pstmt.addBatch();
			needsFlush=true;
		} catch (SQLException e) {
			LOG.error(e.getMessage());
		}
	}
	
	@Override
	public void flush() {
		try {
			if(needsFlush) {
				int[] results=pstmt.executeBatch();
				connection.commit();
				needsFlush=false;
				LOG.info("rows inserted: "+results.length);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
	}
	public StockTradeDaoJdbc(Connection connection) throws SQLException {
		this.connection = connection;
		String sql = "insert into stocktrade(" + "id,price,quantity,symbol,trade) values" + "(?,?,?,?,?)";
		pstmt = this.connection.prepareStatement(sql);
		needsFlush=false;
	}
}
