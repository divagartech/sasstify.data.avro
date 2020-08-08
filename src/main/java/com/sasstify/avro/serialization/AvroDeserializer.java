package com.sasstify.avro.serialization;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class AvroDeserializer<T extends SpecificRecordBase & SpecificRecord> {

    private Class<T> type;
    private static Logger logger = LoggerFactory.getLogger(AvroDeserializer.class);


    public AvroDeserializer(Class<T> type) {
        this.type = type;
    }

    public T deSerializeFromJSON(Schema schema, byte[] data) {
        DatumReader<T> reader = new SpecificDatumReader<>(type);
        Decoder decoder = null;
        try {
            decoder = DecoderFactory.get().jsonDecoder(schema, new String(data));
            return reader.read(null, decoder);
        } catch (IOException e) {
            logger.error("Deserialization error" + e.getMessage());
        }
        return null;
    }

    public T deSerializeFromBSON(byte[] data) {
        DatumReader<T> reader = new SpecificDatumReader<>(type);
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        try {
            return reader.read(null, decoder);
        } catch (IOException e) {
            logger.error("Deserialization error" + e.getMessage());
        }
        return null;
    }

    public DataFileReader<T> deSerealizeFromFile(File file) {
        DatumReader<T> reader = new SpecificDatumReader<T>(type);
        DataFileReader<T> dataFileReader = null;
        try {
            dataFileReader = new DataFileReader<T>(file, reader);
        } catch (IOException e) {
            logger.error("Deserialization error" + e.getMessage());
        }
        return dataFileReader;
    }
}
