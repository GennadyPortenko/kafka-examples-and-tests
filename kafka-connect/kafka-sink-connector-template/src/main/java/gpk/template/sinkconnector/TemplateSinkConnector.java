package gpk.template.sinkconnector;

import gpk.template.config.TemplateConnectorConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import gpk.template.util.VersionUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
public class TemplateSinkConnector extends SinkConnector {
    private Map<String, String> configProperties;

    @Override
    public void initialize(ConnectorContext ctx) {
        super.initialize(ctx);
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            configProperties = props;
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start connector due to configuration error", e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TemplateSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        Map<String, String> taskConfig = new HashMap<>(configProperties);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(taskConfig);
        }
        return configs;
    }

    @Override
    public void stop() {
        log.info("Sink connector stopped");
    }

    @Override
    public ConfigDef config() {
        return TemplateConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }
}
