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
		register("avro", (new StringHolder()).getSchema());


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
		
		 return (new StringHolder()).getSchema();

		
	}

	@Override
	public SchemaDetails<Schema> getLatestSchemaByTopic(String topic) {
		return new SchemaDetails<Schema>(topic, "id",
				(new StringHolder()).getSchema());


	}
}
