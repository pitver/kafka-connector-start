package ru.vershinin.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vershinin.util.Version;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Map.entry;
import static ru.vershinin.sink.MySinkConnectorConfig.TASK_ID;
import static ru.vershinin.sink.MySinkConnectorConfig.TASK_MAX;

public class MySinkConnector extends SinkConnector {
    private static Logger log = LoggerFactory.getLogger(MySinkConnector.class);


    private Map<String, String> configProps;

    @Override
    public void start(final Map<String, String> properties) {
        log.info("Starting LogSinkConnector with properties {}", properties);
        configProps = properties;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MySinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        log.info("Setting task configurations for {} workers.", maxTasks);

        return IntStream.range(0, maxTasks)
                .mapToObj(i -> Map.ofEntries(
                        entry(TASK_ID, String.valueOf(i)),
                        entry(TASK_MAX, String.valueOf(maxTasks))))
                .collect(Collectors.toCollection(() -> new ArrayList<>(maxTasks)));
    }

    @Override
    public void stop() {
        log.info("Stopping LogSinkConnector.");
    }

    @Override
    public ConfigDef config() {
        return MySinkConnectorConfig.config();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
