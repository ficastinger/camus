package com.linkedin.camus.example;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import kafka.message.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Properties;

import org.apache.log4j.Logger;

public class StringDecoder extends MessageDecoder<byte[], Record> {

	
	   private static final Logger sLogger = Logger.getLogger(StringDecoder.class);
	   
    public static final Text YEAR = new Text("year");
    public static final Text MONTH = new Text("month");
    public static final Text DAY = new Text("day");
    public static final Text HOUR = new Text("hour");
    
    Schema schema = Schema.parse("{\"type\": \"record\", " +
            "\"name\": \"StringHolder\", " +
            "\"fields\": " +
            "[{\"name\":\"value\", \"type\": \"string\"}]}");

    @Override
    public void init(Properties props, String topicName) {
        super.init(props, topicName);
    }

    @Override
    public CamusWrapper<Record> decode(byte[] message) {
        Record record = new GenericData.Record(schema);
        String payload = new String(message);
        record.put("value", "payload: " + payload);

        sLogger.warn("pay " + payload);
        
//        File f = new File("/tmp/testlog.log");
//        FileOutputStream fos=null;
//		try {
//			fos = new FileOutputStream(f);
//			 try {
//				fos.write(payload.getBytes());
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//       
//		 try {
//			if(fos!=null) fos.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
        // This would extract the timestamp from the record
        long timestamp = System.currentTimeMillis();
        DateTime d = new DateTime(timestamp);

        CamusWrapper<Record> wrapper = new CamusWrapper<Record>(record, timestamp);

        // extract values for partitioning
//        wrapper.getRecord().put(YEAR.toString(), new IntWritable(d.getYear()));
//        wrapper.getRecord().put(MONTH.toString(), new IntWritable(d.getMonthOfYear()));
//        wrapper.getRecord().put(DAY.toString(), new IntWritable(d.getDayOfMonth()));
//        wrapper.getRecord().put(HOUR.toString(), new IntWritable(d.getHourOfDay()));

      wrapper.put(YEAR, new IntWritable(d.getYear()));
      wrapper.put(MONTH, new IntWritable(d.getMonthOfYear()));
      wrapper.put(DAY, new IntWritable(d.getDayOfMonth()));
      wrapper.put(HOUR, new IntWritable(d.getHourOfDay()));

        
        return wrapper;
    }

}
