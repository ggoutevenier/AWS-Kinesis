package com.datakinesis;
import java.util.UUID;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public class KinesisConsumer {
	private Worker worker = null;
	public KinesisConsumer(
		    ProfileCredentialsProvider credentials,
			String streamName,
			String appName,
			String appVersion,
			String regionName,
			Serde  serde,	
			ObjectDao objectDao
    ) throws Exception {
			
	        String workerId = String.valueOf(UUID.randomUUID());
			ClientConfiguration clientConfiguration = new ClientConfiguration();
	        clientConfiguration.setUserAgent(ClientConfiguration.DEFAULT_USER_AGENT+" "+appName+"/"+appVersion);
	        Region region = RegionUtils.getRegion(regionName);
	        if(region==null) {
	        	throw new Exception(regionName+ " is not a valid region");
	        }
	        KinesisClientLibConfiguration kclConfig =
	                new KinesisClientLibConfiguration(appName, streamName, credentials, workerId)
	            .withRegionName(region.getName())
	            .withCommonClientConfig(clientConfiguration)
	            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

	        IRecordProcessorFactory recordProcessorFactory = new ProcessFactory(objectDao,serde);

	        worker = new Worker(recordProcessorFactory, kclConfig);
	}
	public void run() {
		worker.run();
	}
}
