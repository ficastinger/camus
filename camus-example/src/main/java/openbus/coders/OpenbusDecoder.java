package openbus.coders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import javax.management.RuntimeErrorException;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.log4j.Logger;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;

public class OpenbusDecoder extends MessageDecoder<byte[], Record> {

	
	private static final Logger sLogger = Logger.getLogger(OpenbusDecoder.class);
	   

    @Override
    public void init(Properties props, String topicName) {
    	sLogger.info("init OpenbusDecoder");
        
    	try { 
    		super.init(props, topicName);
    	} catch (Throwable e) {
    		sLogger.warn("error in init OpenbusDecoder" + e.getMessage(), e);
    		throw new RuntimeException("error in init OpenbusDecoder" + e.getMessage(),e);
    	}
        sLogger.info("super init OpenbusDecoder");
    }

    @Override
    public CamusWrapper<Record> decode(byte[] message) {


    	Record record=null;
    	
    	byte[] payload=new byte[message.length - 14];
    	System.arraycopy(message, 14, payload, 0, message.length - 14);
    	
        DatumReader<Record> reader = new GenericDatumReader<Record>();
        ByteArrayInputStream is = new ByteArrayInputStream(payload);
        DataFileStream<Record> dataFileReader;
		try {
			dataFileReader = new DataFileStream<Record>(
			        is, reader);

	        //Record record = new GenericData.Record(dataFileReader.getSchema());
	
	        while (dataFileReader.hasNext()) {
	            record = dataFileReader.next(record);
	        }
	        
	        sLogger.info("Record: " +  record.toString());
	        
	        dataFileReader.close();
	        
        
		} catch (IOException e) {
			// TODO Auto-generated catch block
			sLogger.warn("error in decode OpenbusDecoder" + e.getMessage(), e);

    		throw new RuntimeException("error in decode OpenbusDecoder" + e.getMessage(),e);

		}


        CamusWrapper<Record> wrapper = new CamusWrapper<Record>(record);
        
        return wrapper;
    }

}
