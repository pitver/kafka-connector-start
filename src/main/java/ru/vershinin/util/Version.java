package ru.vershinin.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * вспомогательный класс который в статическом инициализаторе получает из файла version
 */
public final class Version {
    private Version() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Version.class);
    private static final String PATH = "/kafka-connect-version.properties";
    private static String version = "unknown";

    static {
        try (InputStream stream = Version.class.getResourceAsStream(PATH)) {
            Properties props = new Properties();
            props.load(stream);
            version = props.getProperty("version", version).trim();
        } catch (Exception e) {
            LOGGER.warn("Error while loading version:", e);
        }
    }

    public static String getVersion() {
        return version;
    }
}