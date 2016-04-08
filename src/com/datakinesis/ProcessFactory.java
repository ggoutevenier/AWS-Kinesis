package com.datakinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class ProcessFactory implements IRecordProcessorFactory {
	private ObjectDao objectDao;
	private Serde serde;
	public ProcessFactory(ObjectDao objectDao,Serde serde) {
		super();
		this.objectDao=objectDao;
		this.serde=serde;
	}
	@Override
	public IRecordProcessor createProcessor() {
		IRecordProcessor processor=null;
		try {
			System.out.println("createProcessor");
			processor= new Processor(objectDao,serde);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return processor;
	}

}
