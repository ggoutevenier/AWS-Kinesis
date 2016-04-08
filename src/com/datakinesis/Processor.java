package com.datakinesis;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

public class Processor implements IRecordProcessor {
		private ObjectDao objectDao;
		private Serde serde;
	    private static final Log LOG = LogFactory.getLog(Processor.class);
	    private String kinesisShardId;
//	    private int recordsRead,recordsLoaded;
	    private int counter=0;
	    // Reporting interval
	    private static final long REPORTING_INTERVAL_MILLIS = 60000L; // 1 minute
	    private long nextReportingTimeInMillis;

	    // Checkpointing interval
	    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L; // 1 minute
	    private long nextCheckpointTimeInMillis;

	    // Aggregates stats for stock trades
//	    private StockStats stockStats = new StockStats();

	    /**
	     * {@inheritDoc}
	     */
	    @Override
	    public void initialize(String shardId) {
	        LOG.info("Initializing record processor for shard: " + shardId);
	        this.kinesisShardId = shardId;
	        nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
	        nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
	    }

	    private void reportStats() {
//	    	LOG.info("Records loaded,records read: "+recordsLoaded+","+recordsRead);
	    }

	    private void resetStats() {
//	        recordsRead=recordsLoaded=0;
	    }

	    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
	    	LOG.info("Checkpointing shard " + kinesisShardId);
			objectDao.flush();	    	
	        try {
	            checkpointer.checkpoint();
	        } catch (ShutdownException se) {
	            // Ignore checkpoint if the processor instance has been shutdown (fail over).
	            LOG.info("Caught shutdown exception, skipping checkpoint.", se);
	        } catch (ThrottlingException e) {
	            // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
	            LOG.error("Caught throttling exception, skipping checkpoint.", e);
	        } catch (InvalidStateException e) {
	            // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
	            LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
	        }
	    }

	    /**
	     * {@inheritDoc}
	     */
	    @Override
	    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
//			LOG.info("records.size(): "+records.size());
	    	for (Record record : records) {
	    		if(record!=null && record.getData()!=null) {
	    			Object object = serde.deserialize(record.getData().array());
//	    			LOG.info("Counter: "+counter);
	    			objectDao.add(object);
	    			counter++;
	    			if(counter%100==0) {
	    				checkpoint(checkpointer);
	    			}
	    		}
	    		else
	    			LOG.info("record null");
	    	}
	        checkpoint(checkpointer);
	        
	        // If it is time to report stats as per the reporting interval, report stats
	        if (System.currentTimeMillis() > nextReportingTimeInMillis) {
	            reportStats();
	            resetStats();
	            nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
	        }

	        // Checkpoint once every checkpoint interval
	        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
	            checkpoint(checkpointer);
	            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
	        }
	    }
		

	   /**
	    * {@inheritDoc}
	    */
	    @Override
	    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
		        LOG.info("Shutting down record processor for shard: " + kinesisShardId);
		        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
		        if (reason == ShutdownReason.TERMINATE) {
		            checkpoint(checkpointer);
		        }
		    }
	    public Processor(ObjectDao objectDao,Serde serde) throws Exception {
	    	this.objectDao=objectDao;
	    	this.serde=serde;
	    	if(this.serde==null)
	    		throw new Exception("parser not ndefined");
	    	if(this.objectDao==null)
	    		throw new Exception("objectDao not ndefined");
	    }
	
}
