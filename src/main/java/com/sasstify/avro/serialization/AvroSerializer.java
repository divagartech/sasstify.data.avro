package com.sasstify.avro.serialization;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

public class AvroSerializer<T extends SpecificRecordBase & SpecificRecord> {


    private Class<T> type;
    private static final Logger logger = LoggerFactory.getLogger(AvroSerializer.class);

    public AvroSerializer(Class<T> type) {
        this.type = type;
    }

    public byte[] serealizeToJSON(Schema schema, T  record) {
        DatumWriter<T> writer = new SpecificDatumWriter<>(type);
        byte[] data = new byte[0];
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder = null;
        try {
            jsonEncoder = EncoderFactory.get().jsonEncoder(schema, stream);
            data = avroToByte(jsonEncoder, writer, stream, record);
        } catch (IOException e) {
            logger.error("Serialization error " + e.getMessage());
        }
        return data;
    }

    public byte[] serealizeToBSON(T  record) {
        DatumWriter<T> writer = new SpecificDatumWriter<>(type);
        byte[] data = new byte[0];
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder = EncoderFactory.get()
                .binaryEncoder(stream, null);
        try {
            data = avroToByte(jsonEncoder, writer, stream, record);
        } catch (IOException e) {
            logger.error("Serialization error " + e.getMessage());
        }

        return data;
    }

    public void serealizeToFile(Schema schema, List<T> records, File file) {
        DatumWriter<T> writer = new SpecificDatumWriter<>(type);
        DataFileWriter<T> fileWriter = new DataFileWriter<>(writer);
        try {
            fileWriter.create(schema, file);
            for (T record : records) {
                fileWriter.append(record);
            }
            fileWriter.close();
        } catch (IOException e) {
            logger.error("Serialization error " + e.getMessage());
        }
    }

    private byte[] avroToByte(Encoder jsonEncoder, DatumWriter<T> writer, ByteArrayOutputStream stream, T record) throws IOException {
        writer.write(record, jsonEncoder);
        jsonEncoder.flush();
        return stream.toByteArray();
    }
}
