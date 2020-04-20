package com.ibm.eventstreams.connect.jdbcsink;

import com.ibm.eventstreams.connect.jdbcsink.sink.JDBCSinkConfig;
import com.ibm.eventstreams.connect.jdbcsink.sink.JDBCSinkTask;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JDBCSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(JDBCSinkConnector.class);

    // TODO: check with ibm-messaging about externalizing snapshot version
    public static String VERSION = "0.0.1-SNAPSHOT";

    private Map<String, String> props;

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override public String version() {
        return VERSION;
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    @Override public void start(Map<String, String> props) {
        this.props = props;
    }

    /**
     * Returns the Task implementation for this Connector.
     */
    @Override public Class<? extends Task> taskClass() {
        return JDBCSinkTask.class;
    }

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most count configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    @Override public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.nCopies(maxTasks, props);
    }

    /**
     * Stop this connector.
     */
    @Override public void stop() {

    }

    /**
     * Define the configuration for the connector.
     * @return The ConfigDef for this connector.
     */
    @Override public ConfigDef config() {
        return JDBCSinkConfig.config();
    }

    /**
     * Provides a default validation implementation which returns a list of allowed configurations
     * together with configuration errors and recommended values for each configuration.
     *
     * @param connectorConfigs connector configuration values
     * @return list of allowed configurations
     */
    @Override public Config validate(Map<String, String> connectorConfigs) {
        return super.validate(connectorConfigs);
    }

}