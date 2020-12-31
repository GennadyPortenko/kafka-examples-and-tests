package gpk.template.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

@Slf4j
public class TemplateConnectorConfig extends AbstractConfig {

    public static final String GREETING = "example.greeting";
    public static final String ID = "example.id";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(GREETING, ConfigDef.Type.STRING, "hi", new ConfigDef.NonEmptyStringWithoutControlChars(), ConfigDef.Importance.HIGH, "Greeting string")
            .define(ID, ConfigDef.Type.INT, 0, new ConfigDef.NonNullValidator(), ConfigDef.Importance.HIGH, "ID");

    public TemplateConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }
}

