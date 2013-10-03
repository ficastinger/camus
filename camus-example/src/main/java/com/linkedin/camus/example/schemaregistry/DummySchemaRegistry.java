package com.linkedin.camus.example.schemaregistry;

import org.apache.avro.Schema;

import com.linkedin.camus.example.records.StringHolder;
import com.linkedin.camus.schemaregistry.MemorySchemaRegistry;

/**
 * This is a little dummy registry that just uses a memory-backed schema
 * registry to store two dummy Avro schemas. You can use this with
 * camus.properties
 */
public class DummySchemaRegistry extends MemorySchemaRegistry<Schema> {
//	public DummySchemaRegistry(Configuration conf) {
	public DummySchemaRegistry() {
		super();
		//super.register("test", StringHolder.newBuilder().build().getSchema());
		super.register("test", (new StringHolder("test")).getSchema());
		super.register("avrotest", (new StringHolder("test")).getSchema());		
//		super.register("DUMMY_LOG", DummyLog.newBuilder().build().getSchema());
//		super.register("DUMMY_LOG_2", DummyLog2.newBuilder().build()
//				.getSchema());
	}
}
