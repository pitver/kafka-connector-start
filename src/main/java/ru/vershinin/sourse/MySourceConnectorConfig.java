package ru.vershinin.sourse;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MySourceConnectorConfig extends AbstractConfig {

    /**
     * URL-адрес конечной точки API, из которой мы хотим получить данные
     */

    public static final String URI_TO_ANALYZE = "uri.to.analyze";
    public static final String API_ENDPOINT_DOC = "API URL To analyze";

    /**
     * имя темы Kafka, в которую мы хотим записать данные
     */
    public static final String OUTPUT_TOPIC_NAME = "output.topic.name";
    public static final String TOPIC_DOC = "Topic to write to";

    public MySourceConnectorConfig(Map<String, String> props) {
        super(config(), props);

    }


    /**
     * Метод используется для указания набора ожидаемых конфигураций. Для каждой конфигурации вы можете указать:
     * имя, тип, значение по умолчанию, документацию, информацию о группе, порядок в группе, ширину значения конфигурации и имя,
     * подходящее для отображения в пользовательском интерфейсе. Вы можете предоставить специальную логику проверки,
     * используемую для проверки единой конфигурации, путем переопределения ConfigDef.Validator.
     * Кроме того, вы можете указать иждивенцев конфигурации. Допустимые значения и видимость конфигурации могут изменяться в соответствии
     * со значениями других конфигураций. Вы можете переопределить, ConfigDef.Recommender, чтобы получить допустимые значения и установить
     * видимость конфигурации с учетом текущих значений конфигурации.
     *
     * @return - ConfigDef
     * https://kafka.apache.org/11/javadoc/org/apache/kafka/common/config/ConfigDef.html
     */
    public static ConfigDef config() {
          return new ConfigDef()
                  .define(URI_TO_ANALYZE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, API_ENDPOINT_DOC)
                  .define(OUTPUT_TOPIC_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TOPIC_DOC);
    }

}
