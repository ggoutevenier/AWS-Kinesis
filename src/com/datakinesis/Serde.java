package com.datakinesis;

public interface Serde {
	public byte[] serialize(Object object);
	public Object deserialize(byte[] bytes);
}