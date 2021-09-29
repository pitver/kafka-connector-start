package ru.vershinin.sourse;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vershinin.util.Version;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static ru.vershinin.sourse.MySourceConnectorConfig.OUTPUT_TOPIC_NAME;
import static ru.vershinin.sourse.MySourceConnectorConfig.URI_TO_ANALYZE;


/**
 * @link https://kafka.apache.org/0100/javadoc/org/apache/kafka/connect/connector/Task.html
 */
public class MySourceTask extends SourceTask {
    MySourceConnectorConfig mySourceConnectorConfig;
    private static Logger logger = LoggerFactory.getLogger(MySourceTask.class);
    /**
     * тема для письма
     */
    private String outputTopicName;
    private Connection connection;

    /**
     * исходный раздел (например, имя файла или имя таблицы), чтобы различать источник, из которого пришла запись
     */
    private Map<String, Object> sourcePartition;

    /**
     * исходное смещение (например, позиция в файле или значение в столбце временной метки таблицы)
     * для возобновления потребления данных в случае перезапуска
     */
    private Map<String, Object> sourceOffset;

    private final int timeOut = 3000;
    private final int timeSleep = 5000;

    @Override
    public String version() {
        return Version.getVersion();
    }

    /**
     * Метод подготавливает переменные получая их из класса конфигурации
     *
     * @param props -список свойств
     */
    @Override
    public void start(Map<String, String> props) {

        logger.info("Starting source task with properties {}", props);
        mySourceConnectorConfig = new MySourceConnectorConfig(props);
        String uriToAnalyze = props.get(URI_TO_ANALYZE);
        outputTopicName = props.get(OUTPUT_TOPIC_NAME);
        sourcePartition = Collections.singletonMap("uriToAnalyze", uriToAnalyze);
        connection = Jsoup.connect(uriToAnalyze);
    }

    /**
     * Данный метод формирует список сообщений которые будут отправлены в topic
     *
     * @return List<SourceRecord> result
     * @throws InterruptedException
     * @link https://kafka.apache.org/20/javadoc/index.html?org/apache/kafka/connect/source/package-summary.html
     * @link https://kafka.apache.org/20/javadoc/org/apache/kafka/connect/source/SourceRecord.html
     * @link https://kafka.apache.org/20/javadoc/org/apache/kafka/connect/data/Schema.html
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        List<SourceRecord> result = new ArrayList<>();

        try {
            final Document document = connection.timeout(timeOut).get();
            final String localDateTime = LocalDateTime.now().toString();
            final Elements elements = document.select("a[href]");

            if (!elements.isEmpty()) {
                int bound = elements.size();
                IntStream.range(0, bound).forEach(i -> {
                    final var element = elements.get(i);
                    sourceOffset = Collections.singletonMap("linkOffset", i);
                    var sourceRecord = new SourceRecord(sourcePartition, sourceOffset, outputTopicName,
                            Schema.STRING_SCHEMA, localDateTime, Schema.STRING_SCHEMA, element.attr("href"));
                    result.add(sourceRecord);
                });

            }
            Thread.sleep(timeSleep);

        } catch (IOException e) {
            logger.info(e.getMessage());
            e.printStackTrace();
        }

        logger.info("poll complete with " + result);
        return result;
    }

    @Override
    public void stop() {
        logger.info("sourceTask stop");
    }
}
