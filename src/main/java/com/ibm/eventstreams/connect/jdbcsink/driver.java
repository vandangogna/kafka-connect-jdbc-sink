package com.ibm.eventstreams.connect.jdbcsink;

import java.io.File;
import java.io.InputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import com.ibm.db2.cmx.internal.json4j.JSONArray;
import com.ibm.db2.cmx.internal.json4j.JSONObject;
import com.ibm.eventstreams.connect.jdbcsink.JDBCSinkConfig;
import com.ibm.eventstreams.connect.jdbcsink.database.DatabaseFactory;
import com.ibm.eventstreams.connect.jdbcsink.database.IDatabase;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;

public class driver {

    public static void main(String[] args) {
        String dbConnConfig = "/Users/vandan.gognaibm.com/Development/cloudGarage/tch/vg/kafka-connect-jdbc-sink/src/main/java/com/ibm/eventstreams/connect/jdbcsink/111driver/db.config.properties";
        try {
            DBConnector dbConn = new DBConnector(dbConnConfig);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main2(String[] args) {
        System.out.println("Driver Started..");

        FileReader reader;
        Properties dbConfigProps = new Properties();
        try {
            reader = new FileReader(new File("").getAbsolutePath()
                    + "/src/main/java/com/ibm/eventstreams/connect/jdbcsink/driver/db.config.properties");
            dbConfigProps = new Properties();
            dbConfigProps.load(reader);

            HashMap<String, String> props = new HashMap<String, String>();
            props.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_URL, dbConfigProps.getProperty("url"));
            props.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_USER, dbConfigProps.getProperty("user"));
            props.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_PASSWORD, dbConfigProps.getProperty("password"));
            props.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_DS_POOL_SIZE, dbConfigProps.getProperty("poolsize"));
            // props.put(JDBCSinkConfig.CONFIG_NAME_TABLE_NAME_FORMAT,
            // dbConfigProps.getProperty("tablenameformat"));
            JDBCSinkConfig config = new JDBCSinkConfig(props);

            DatabaseFactory databaseFactory = new DatabaseFactory();

            IDatabase database = databaseFactory.makeDatabase(config);
            if (database == null) {
                throw new Exception("DB connection FAILED");
            } else {
                System.out.println("DB connection established.");
            }

            ArrayList<SinkRecord> records = new ArrayList<SinkRecord>();

            try {

                Schema dateSchema = getSchema();

                org.apache.kafka.connect.data.Struct payload = new org.apache.kafka.connect.data.Struct(dateSchema);
                buildSamplePayload(payload);

                SinkRecord rec = new SinkRecord("topic", 1, null, null, dateSchema, payload, 0);

                for (int i = 0; i < 1; i++) {
                    records.add(rec);
                }
                System.out.println("records: " + records.size());
                database.getWriter().insert("SWZ72674.TEST_TABLE", records);
                System.out.println(String.format("%d RECORDS PROCESSED", records.size()));
            } catch (Exception error) {
                System.out.println("Failed to insert records: " + error);
                // TODO: throw exception to cancel execution or retry?
            }

            // ("topic", 1, null, null, null, null, "", 0)
        } catch (Exception e) {
            System.out.println("Failed to build the database {} " + e);
            e.printStackTrace();
        }
    }

    private static Schema getSchema() throws Exception {
        String result = "";
        SchemaBuilder builder = SchemaBuilder.struct();
        try {
            // SchemaBuilder dbSchema = Schema.Type.ARRAY
            // //.name("com.example.CalendarDate").version(2).doc("A calendar date including
            // month, day, and year.")
            // .field("dont", Schema.STRING_SCHEMA)
            // .field("abc", Schema.STRING_SCHEMA)
            // .field("foo", Schema.STRING_SCHEMA)
            // .field("etc", Schema.STRING_SCHEMA)
            // .build();
            result = new String(Files.readAllBytes(Paths.get(
                    "/Users/vandan.gognaibm.com/Development/cloudGarage/tch/vg/kafka-connect-jdbc-sink/src/main/java/com/ibm/eventstreams/connect/jdbcsink/driver/data2.json")));
            JSONArray payload = JSONArray.parse(result);

            for (int i = 0; i < payload.size(); i++) {

                JSONObject element = (JSONObject) payload.get(i);
                String valueType = element.get("type").toString();
                switch (element.get("type").toString().toLowerCase()) {
                    case "string":
                        builder.field(element.get("name").toString(), Schema.STRING_SCHEMA);
                        break;
                    case "boolean":
                        builder.field(element.get("name").toString(), Schema.BOOLEAN_SCHEMA);
                        break;
                    case "double":
                        builder.field(element.get("name").toString(), Schema.FLOAT64_SCHEMA);
                        break;
                    default:
                        throw new Exception("Unrecognized data type found");
                }
            }
            builder.build();
            System.out.print(result);
        } catch (Exception ex) {
            throw ex;
        }
        return builder; // SchemaBuilder.array(builder).build();
    }

    private static void buildSamplePayload(org.apache.kafka.connect.data.Struct payload) throws Exception {
        try {
            String result = new String(Files.readAllBytes(Paths.get(
                    "/Users/vandan.gognaibm.com/Development/cloudGarage/tch/vg/kafka-connect-jdbc-sink/src/main/java/com/ibm/eventstreams/connect/jdbcsink/driver/payload.json")));
            JSONObject payloadObject = JSONObject.parse(result);
            for (Object entry : payloadObject.entrySet()) {
                Entry<String, Object> element = (Entry<String, Object>) entry;
                payload.put(element.getKey().toString(), element.getValue());
            }
        } catch (Exception ex) {
            throw ex;
        }
    }
}

class DBConnector {
    File dbConfig;
    Properties dbConfigProps = new Properties();
    IDatabase database = null;

    public DBConnector(String absolutePath) throws Exception {
        try {
            dbConfig = new File(absolutePath);
            if (dbConfig.exists() == false) {
                throw new IOException("Failed to find DB configuration");
            }
            FileReader reader = new FileReader(dbConfig);
            dbConfigProps.load(reader);
            HashMap<String, String> sinkProps = new HashMap<String, String>();
            sinkProps.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_URL, dbConfigProps.getProperty("url"));
            sinkProps.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_USER, dbConfigProps.getProperty("user"));
            sinkProps.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_PASSWORD, dbConfigProps.getProperty("password"));
            sinkProps.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_DS_POOL_SIZE, dbConfigProps.getProperty("poolsize"));
            sinkProps.put(JDBCSinkConfig.CONFIG_NAME_TABLE_NAME_FORMAT, dbConfigProps.getProperty("tablenameformat"));
            //create the db connection.
            makeDatabase(new JDBCSinkConfig(sinkProps));
        } catch (Exception ex) {
            throw ex;
        }
    }

    private void makeDatabase(JDBCSinkConfig config) throws Exception {
        DatabaseFactory databaseFactory = new DatabaseFactory();

        database = databaseFactory.makeDatabase(config);
        if (database == null) {
            throw new Exception("DB connection FAILED");
        } else {
            System.out.println("DB connection established.");
        }
        insertRecords(10);
    }

    public void insertRecords(int numRecords) throws Exception{
        ArrayList<SinkRecord> records = new ArrayList<SinkRecord>();
        try {

            Schema dateSchema = getSchema();
            org.apache.kafka.connect.data.Struct payload = new org.apache.kafka.connect.data.Struct(dateSchema);
            buildSamplePayload(payload);

            SinkRecord rec = new SinkRecord("topic", 1, null, null, dateSchema, payload, 0);

            for (int i = 0; i < 1; i++) {
                records.add(rec);
            }
            System.out.println("records: " + records.size());
            database.getWriter().insert("SWZ72674.TEST_TABLE", records);
            System.out.println(String.format("%d RECORDS PROCESSED", records.size()));
        } 
        catch(Exception ex){
            throw ex;
        }
    }


    private Schema getSchema() throws Exception {
        String result = "";
        SchemaBuilder builder = SchemaBuilder.struct();
        try {
            result = new String(Files.readAllBytes(Paths.get(
                    "/Users/vandan.gognaibm.com/Development/cloudGarage/tch/vg/kafka-connect-jdbc-sink/src/main/java/com/ibm/eventstreams/connect/jdbcsink/111driver/data2.json")));
            List<Map<String, Object>> payload = JSONArray.parse(result);

            for( Map<String, Object> element : payload){
            
                String valueType = element.get("type").toString().toLowerCase();
                String fieldName = element.get("name").toString();
                //switch (element.get("type").toString().toLowerCase()) {
                switch (valueType) {
                    case "string":
                        builder.field(fieldName, Schema.STRING_SCHEMA);
                        break;
                    case "boolean":
                        builder.field(fieldName, Schema.BOOLEAN_SCHEMA);
                        break;
                    case "double":
                        builder.field(fieldName, Schema.FLOAT64_SCHEMA);
                        break;
                    default:
                        throw new Exception("Unrecognized data type found");
                }
            }
            builder.build();
            System.out.print(result);
        } catch (Exception ex) {
            throw ex;
        }
        return builder;
    }

    private  void buildSamplePayload(org.apache.kafka.connect.data.Struct payload) throws Exception {
        try {
            String result = new String(Files.readAllBytes(Paths.get(
                    "/Users/vandan.gognaibm.com/Development/cloudGarage/tch/vg/kafka-connect-jdbc-sink/src/main/java/com/ibm/eventstreams/connect/jdbcsink/111driver/payload.json")));
            JSONObject payloadObject = JSONObject.parse(result);
            for (Object entry : payloadObject.entrySet()) {
                Entry<String, Object> element = (Entry<String, Object>) entry;
                payload.put(element.getKey().toString(), element.getValue());
            }
        } catch (Exception ex) {
            throw ex;
        }
    }

}
