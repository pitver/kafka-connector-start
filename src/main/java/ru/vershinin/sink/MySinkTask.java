package ru.vershinin.sink;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vershinin.util.Version;

import java.util.Collection;
import java.util.Map;

public class MySinkTask extends SinkTask {
    private static Logger log= LoggerFactory.getLogger(MySinkTask.class);

    @Override
    public String version() {
        return Version.getVersion();
    }

    private MySinkConnectorConfig.LogLevel logLevel;
    private MySinkConnectorConfig.LogContent logContent;
    private String logPatternFormat;


    /**
     * @param properties
     */
    @Override
    public void start(final Map<String, String> properties) {
        final MySinkConnectorConfig config = new MySinkConnectorConfig(properties);
        logLevel = config.getLogLevel();
        logContent = config.getLogContent();
        logPatternFormat = config.getLogPatternFormat();
        log.info("Starting LogSinkTask with properties {}", properties);
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        switch (logLevel) {
            case INFO:
                records.forEach(record -> log.info(logPatternFormat,
                        getLoggingArgs(logContent, record)));
                break;
            case WARN:
                records.forEach(record -> log.warn(logPatternFormat,
                        getLoggingArgs(logContent, record)));
                break;
            case DEBUG:
                records.forEach(record -> log.debug(logPatternFormat,
                        getLoggingArgs(logContent, record)));
                break;

            case TRACE:
                records.forEach(record -> log.trace(logPatternFormat,
                        getLoggingArgs(logContent, record)));
                break;

            case ERROR:
                records.forEach(record -> log.error(logPatternFormat,
                        getLoggingArgs(logContent, record)));
                break;

        }
    }

    private Object[] getLoggingArgs(final MySinkConnectorConfig.LogContent logContent, final SinkRecord record) {
        switch (logContent) {
            case KEY:
                return new Object[]{record.key(), StringUtils.EMPTY};
            case VALUE:
                return new Object[]{record.value(), StringUtils.EMPTY};
            case KEY_VALUE:
                return new Object[]{record.key(), record.value()};
            default:
                // case ALL
                return new Object[]{record, StringUtils.EMPTY};
        }
    }


    @Override
    public void stop() {
        log.info("Stopping LogSinkTask.");
    }
}
