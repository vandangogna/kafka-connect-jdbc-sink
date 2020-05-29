package com.ibm.eventstreams.connect.jdbcsink;

import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.print.DocFlavor.STRING;

import com.ibm.db2.cmx.PushDownError.SQLException;
import com.ibm.db2.cmx.internal.json4j.JSONArray;
import com.ibm.db2.cmx.internal.json4j.JSONObject;
import com.ibm.eventstreams.connect.jdbcsink.database.DatabaseFactory;
import com.ibm.eventstreams.connect.jdbcsink.database.IDatabase;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;

public class driver {

    public static void main(String[] args) {
        System.out.println("Driver Started..");
        HashMap <String, String> props = new HashMap<String, String>();
        props.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_URL, "jdbc:db2://dashdb-txn-sbox-yp-dal09-04.services.dal.bluemix.net:50001/BLUDB:sslConnection=true;");
        props.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_USER, "swz72674");
        props.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_PASSWORD,"dhj4-vx7h2p99fl9");
        props.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_DS_POOL_SIZE, "1");
        props.put(JDBCSinkConfig.CONFIG_NAME_TABLE_NAME_FORMAT,"SWZ72674");
        JDBCSinkConfig config = new JDBCSinkConfig(props);

        DatabaseFactory databaseFactory = new DatabaseFactory();
        try {
            IDatabase database = databaseFactory.makeDatabase(config);
            if (database == null){
                throw new Exception("DB connection FAILED");
            } else{
                System.out.println("DB connection established.");
            }
        
            // TODO: add records
          
            ArrayList<SinkRecord> records = new ArrayList<SinkRecord>();
          
            try {
                
                Schema dateSchema = getSampleData();
                //SchemaBuilder.struct()
                //.name("com.example.CalendarDate").version(2).doc("A calendar date including month, day, and year.")
                // .field("dont", Schema.STRING_SCHEMA)
                // .field("abc", Schema.STRING_SCHEMA)
                // .field("foo", Schema.STRING_SCHEMA)
                // .field("etc", Schema.STRING_SCHEMA)
                // .build();

                org.apache.kafka.connect.data.Struct str = new org.apache.kafka.connect.data.Struct(dateSchema);
                str.put("dont", "whatever");
                str.put("abc", "42");
                str.put("foo", "true");
                str.put("etc", "etc");
                final Map<String, Object> value = new HashMap<String, Object>();
                value.put("dont", "whatever");
                value.put("abc", 42);
                value.put("foo", true);
                value.put("etc", "etc");
                SinkRecord rec = new SinkRecord("topic", 1, null, null, dateSchema, str, 0);
                records.add(rec);
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

    private static Schema getSampleData() throws Exception {
        String result = "";
        SchemaBuilder builder = SchemaBuilder.struct();
        try{
            //SchemaBuilder dbSchema = Schema.Type.ARRAY
            //     //.name("com.example.CalendarDate").version(2).doc("A calendar date including month, day, and year.")
            //     .field("dont", Schema.STRING_SCHEMA)
            //     .field("abc", Schema.STRING_SCHEMA)
            //     .field("foo", Schema.STRING_SCHEMA)
                //  .field("etc", Schema.STRING_SCHEMA)
                //  .build();
            result = new String(Files.readAllBytes(Paths.get("/Users/vandan.gognaibm.com/Development/cloudGarage/tch/vg/kafka-connect-jdbc-sink/src/main/java/com/ibm/eventstreams/connect/jdbcsink/data2.json")));
            JSONArray payload =  JSONArray.parse(result);
            
            for(int i  = 0; i < payload.size(); i++){
                
                JSONObject element = (JSONObject)payload.get(i);
                String valueType = element.get("type").toString();
                switch(element.get("type").toString().toLowerCase()){
                    case "string":
                        builder.field(element.get("name").toString(), Schema.STRING_SCHEMA);
                        break;
                    case "boolean":
                        builder.field(element.get("name").toString(), Schema.STRING_SCHEMA);
                        break;
                    case "double":
                        builder.field(element.get("name").toString(), Schema.STRING_SCHEMA);
                        break;
                    default:
                        throw new Exception("Unrecognized data type found");
                }
            }
            builder.build();
            System.out.print(result);
       }
        catch(Exception ex){
           throw ex;
        }
        return builder; //SchemaBuilder.array(builder).build();
    }
    
}