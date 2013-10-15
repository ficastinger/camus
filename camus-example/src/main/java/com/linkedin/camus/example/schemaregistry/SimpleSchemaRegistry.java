package com.linkedin.camus.example.schemaregistry;

import java.util.Properties;

import openbus.schema.ApacheLog;

import org.apache.avro.Schema;

import com.linkedin.camus.example.records.StringHolder;
import com.linkedin.camus.schemaregistry.MemorySchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaDetails;
import com.linkedin.camus.schemaregistry.SchemaRegistry;

/**
 * This is a little dummy registry that just uses a memory-backed schema
 * registry to store two dummy Avro schemas. You can use this with
 * camus.properties
 */
public class SimpleSchemaRegistry implements SchemaRegistry<Schema> {
//	public DummySchemaRegistry(Configuration conf) {
	public SimpleSchemaRegistry() {

		//super.register("test", StringHolder.newBuilder().build().getSchema());
		register("stringHolder", (new StringHolder()).getSchema());
		register("apacheLog", (new ApacheLog()).getSchema());	

	}

	@Override
	public void init(Properties props) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public String register(String topic, Schema schema) {
		// TODO Auto-generated method stub
		return "id";
	}

	@Override
	public Schema getSchemaByID(String topic, String id) {
		// TODO Auto-generated method stub
		
		if(topic.startsWith("_1_")) return (new StringHolder()).getSchema();
		else if(topic.startsWith("_2_")) return (new ApacheLog()).getSchema();
		else return (new ApacheLog()).getSchema();	
		
	}

	@Override
	public SchemaDetails<Schema> getLatestSchemaByTopic(String topic) {
		if(topic.startsWith("_1_")) return new SchemaDetails<Schema>(topic, "id",
				(new StringHolder()).getSchema());
		else if(topic.startsWith("_2_")) return new SchemaDetails<Schema>("topic", "id",
				(new ApacheLog()).getSchema());
		else  return new SchemaDetails<Schema>("topic", "id",
				(new ApacheLog()).getSchema());

	}
}
