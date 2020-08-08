package com.sasstify.avro.serialization;

import com.sasstify.avro.model.Active;
import com.sasstify.avro.model.AvroHttpRequest;
import com.sasstify.avro.model.ClientIdentifier;
import org.apache.avro.file.DataFileReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

public class AvroSerealizerDeSerealizerUnitTest {

    AvroSerializer<AvroHttpRequest> serealizer;
    AvroDeserializer<AvroHttpRequest> deSerealizer;
    AvroHttpRequest request;

    @BeforeEach
    public void setUp() throws Exception {
        serealizer = new AvroSerializer<>(AvroHttpRequest.class);
        deSerealizer = new AvroDeserializer<>(AvroHttpRequest.class);

        ClientIdentifier clientIdentifier = ClientIdentifier.newBuilder()
                .setHostName("localhost")
                .setIpAddress("255.255.255.0")
                .build();

        List<CharSequence> employees = new ArrayList();
        employees.add("James");
        employees.add("Alice");
        employees.add("David");
        employees.add("Han");

        request = AvroHttpRequest.newBuilder()
                .setRequestTime(01l)
                .setActive(Active.YES)
                .setClientIdentifier(clientIdentifier)
                .setEmployeeNames(employees)
                .build();
    }

    @AfterEach
    public void tearDown() throws Exception {
    }

    @Test
    public void WhenSerializedUsingJSONEncoder_thenObjectGetsSerialized() {
        byte[] data = serealizer.serealizeToJSON(request.getSchema(),request);
        assertTrue(Objects.nonNull(data));
        assertTrue(data.length > 0);
    }

    @Test
    public void WhenSerializedUsingBinaryEncoder_thenObjectGetsSerialized() {
        byte[] data = serealizer.serealizeToBSON(request);
        assertTrue(Objects.nonNull(data));
        assertTrue(data.length > 0);
    }

    @Test
    public void WhenDeserializeUsingJSONDecoder_thenActualAndExpectedObjectsAreEqual() {
        byte[] data = serealizer.serealizeToJSON(request.getSchema(),request);
        AvroHttpRequest actualRequest = deSerealizer.deSerializeFromJSON(request.getSchema(),data);
        assertEquals(actualRequest, request);
        assertTrue(actualRequest.getRequestTime() == request.getRequestTime());
    }

    @Test
    public void WhenDeserializeUsingBinaryecoder_thenActualAndExpectedObjectsAreEqual() {
        byte[] data = serealizer.serealizeToBSON(request);
        AvroHttpRequest actualRequest = deSerealizer.deSerializeFromBSON(data);
        assertEquals(actualRequest, request);
        assertTrue(actualRequest.getRequestTime() == request.getRequestTime());
    }

    @Test
    public void WhenSerializedDesrializedFromFile_thenActualAndExpectedObjectsAreEqual() {
        File file = new File("src/test/resources/request.avro");
        List<AvroHttpRequest> records = new ArrayList<>();
        records.add(request);
        serealizer.serealizeToFile(request.getSchema(), records, file);
        AvroHttpRequest actualRequest = null;
        try {
            DataFileReader<AvroHttpRequest> data = deSerealizer.deSerealizeFromFile(file);
            while(data.hasNext()) {
                actualRequest = data.next(actualRequest);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        assertEquals(actualRequest, request);
        assertTrue(actualRequest.getRequestTime() == request.getRequestTime());
    }
}
