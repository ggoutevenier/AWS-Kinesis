package com.datakinesis;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
public class KinesisProducer {
	BlockingQueue<Object> queue = new ArrayBlockingQueue<Object>(1000);	
	protected AmazonKinesis kinesisClient;
	private Thread thread=null;
	private boolean end;
	protected String streamName;
	private void validateStream() throws Exception {
		DescribeStreamResult result = kinesisClient.describeStream(this.streamName); 
		if(!result.getStreamDescription().getStreamStatus().equals("ACTIVE")) {
			throw new Exception("Invalid stream: "+streamName);
		}		
	}
	
	private static final Log LOG = LogFactory.getLog(KinesisProducer.class);
	private Serde serde;          
	public KinesisProducer(
		    ProfileCredentialsProvider credentials,
			String streamName,
			String appName,
			String appVersion,
			String regionName,
			Serde serde			
		) throws Exception {
		this.streamName=streamName;

		Region region = RegionUtils.getRegion(regionName);
        if(region==null) {
        	throw new Exception(regionName+ " is not a valid region");
        }
		
		ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setUserAgent(ClientConfiguration.DEFAULT_USER_AGENT+" "+appName+"/"+appVersion);
		kinesisClient = new AmazonKinesisClient(credentials,clientConfiguration);
		kinesisClient.setRegion(region);
		validateStream();
		this.serde=serde;
		end=false;
		// add thread to flush records every second
		thread = new Thread() {
			public void run() {
				while(!end) {
					try {
						sleep(1000); // sleep for 1 second
						LOG.info(queue.size());						
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					flush();
				}
			}
		};
		thread.start();
	}
	public void close() throws InterruptedException {
		end=true;
		while(queue.size()!=0) {
			flush();
		}
	}

    private void reprocessErrors(PutRecordsResult putRecordsResult,List <PutRecordsRequestEntry> putRecordsRequestEntryList) {
    	while (putRecordsResult.getFailedRecordCount() > 0) {
    		PutRecordsRequest putRecordsRequest=new PutRecordsRequest();
    		final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
    	    final List<PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.getRecords();
    	    for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
    	        final PutRecordsRequestEntry putRecordRequestEntry = putRecordsRequestEntryList.get(i);
    	        final PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(i);
    	        if (putRecordsResultEntry.getErrorCode() != null) {
    	            failedRecordsList.add(putRecordRequestEntry);
    	        }
    	    }
    	    putRecordsRequestEntryList = failedRecordsList;
    	    putRecordsRequest.setRecords(putRecordsRequestEntryList);
    	    putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
    	    LOG.info("Reprocess Failed Put Result " + putRecordsResult);
    	}
    }

    public void add(Object object) {
    	queue.add(object);
    }

    private void flush() {
		if(queue.size()>0) {
			LOG.info("start");
			PutRecordsRequest putRecordsRequest=new PutRecordsRequest();
			putRecordsRequest.setStreamName(streamName);
		
			List <PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList<>();
			int i=0;

			Object object;
			object=queue.poll();
			while(object!=null) {
				i++;
				byte []bytes=serde.serialize(object);
				if(bytes==null ) {
					LOG.warn("Could not get JSON bytes for object");
				}
				else {
					PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
					putRecordsRequestEntry.setData(ByteBuffer.wrap(bytes));
					putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", i));
					putRecordsRequestEntryList.add(putRecordsRequestEntry);
				}
				if(i>=500) // process a max of 100 records at a time 
					break;
				object=queue.poll();
			}

			putRecordsRequest.setRecords(putRecordsRequestEntryList);
			PutRecordsResult putRecordsResult  = kinesisClient.putRecords(putRecordsRequest);
			reprocessErrors(putRecordsResult,putRecordsRequestEntryList);		
			LOG.info(streamName+" Records: " + (putRecordsResult.getRecords().size()-putRecordsResult.getFailedRecordCount()));
		}
	}
	
}
