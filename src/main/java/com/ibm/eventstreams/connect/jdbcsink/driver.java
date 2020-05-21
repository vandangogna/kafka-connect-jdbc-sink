package com.ibm.eventstreams.connect.jdbcsink;

import java.util.HashMap;

import com.ibm.eventstreams.connect.jdbcsink.database.DatabaseFactory;
import com.ibm.eventstreams.connect.jdbcsink.database.IDatabase;

import org.apache.kafka.connect.sink.SinkRecord;

public class driver {

    public static void main(String[] args) {
        System.out.println("Driver Started..");
        HashMap <String, String> props = new HashMap<String, String>();
        props.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_URL, "jdbc:db2://150.238.221.213:32277/orderdb:sslConnection=false;");
        props.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_USER, "db2inst1");
        props.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_PASSWORD,"db2inst1");
        props.put(JDBCSinkConfig.CONFIG_NAME_CONNECTION_DS_POOL_SIZE, "1");
        props.put(JDBCSinkConfig.CONFIG_NAME_TABLE_NAME_FORMAT,"LOCAL_TEST");
        JDBCSinkConfig config = new JDBCSinkConfig(props);

        DatabaseFactory databaseFactory = new DatabaseFactory();
        try {
            IDatabase database = databaseFactory.makeDatabase(config);
            if (database == null){
                throw new Exception("No db found.");
            }
            // TODO: add records
        } catch (Exception e) {
            System.out.println("Failed to build the database {} " + e);
            e.printStackTrace();
        }
    }


    
}