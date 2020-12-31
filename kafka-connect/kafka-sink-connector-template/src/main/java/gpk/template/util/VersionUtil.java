package gpk.template.util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Properties;

@Slf4j
public class VersionUtil {
    public static String getVersion() {
        final Properties properties = new Properties();
        try {
            properties.load(VersionUtil.class.getClassLoader().getResourceAsStream("application.properties"));
        } catch (IOException e) {
            log.error("Unable to load connector version from application.properties.", e);
        }
        return properties.getProperty("project.version","0.1");

    }
}
