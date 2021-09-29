package ru.vershinin.sourse;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vershinin.util.Version;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Map.entry;
import static ru.vershinin.sourse.MySourceConnectorConfig.OUTPUT_TOPIC_NAME;
import static ru.vershinin.sourse.MySourceConnectorConfig.URI_TO_ANALYZE;


/**
 * @link https://kafka.apache.org/30/javadoc/org/apache/kafka/connect/connector/Connector.html
 */
public class MySourceConnector extends SourceConnector {

    private static final int COUNT = 3;
    private MySourceConnectorConfig mySourceConnectorConfig;


    private static Logger logger = LoggerFactory.getLogger(MySourceConnector.class);


    /**
     * Метод подготавливает переменные получая их из класса конфигурации
     *
     * @param props -список свойств
     */
    @Override
    public void start(Map<String, String> props) {

        mySourceConnectorConfig = new MySourceConnectorConfig(props);

        logger.info("start with " + props);
    }

    /**
     * Метод возвращает класс с задачами
     *
     * @return MySourceTask.class
     * @see MySourceTask
     */
    @Override
    public Class<? extends Task> taskClass() {
        return MySourceTask.class;
    }

    /**
     * Метод предоставляет набор конфигов для задач. Задачи выполняются в отдельных потоках,
     * поэтому коннектор может выполнять несколько задач параллельно.
     *
     * @param maxTasks - int значение для maxTasks, которое автоматически извлекается из свойств конфигурации,
     *                 которые вы предоставляете для своего настраиваемого коннектора, через .properties файл
     *                 (при запуске коннектора с помощью connect-standalone команды) или через REST API Kafka Connect.
     *                 Вы можете использовать это maxTasks значение, чтобы определить, сколько наборов конфигураций вам понадобится,
     *                 причем каждый набор будет использоваться отдельной задачей.
     * @return -list task
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
       /* Здесь наша задача должна знать две вещи:
        1 - URL-адрес для опроса, чтобы получить случайные значения Long,
        2 - тема кафки, в которую нужно писать*/

        return IntStream.range(0, maxTasks)
                .mapToObj(i -> Map.ofEntries(
                        entry(URI_TO_ANALYZE, mySourceConnectorConfig.getString(URI_TO_ANALYZE)),
                        entry(OUTPUT_TOPIC_NAME, mySourceConnectorConfig.getString(OUTPUT_TOPIC_NAME))))
                .collect(Collectors.toCollection(() -> new ArrayList<>(maxTasks)));

    }

    @Override
    public void stop() {
        logger.info("stop");
    }

    /**
     * Метод возвращает набор конфигураций
     *
     * @return ConfigDef
     * @see MySourceConnectorConfig
     */
    @Override
    public ConfigDef config() {
        return MySourceConnectorConfig.config();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
