
package gpk.template.sinkconnector;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import gpk.template.config.TemplateConnectorConfig;
import gpk.template.util.VersionUtil;

import java.util.Collection;
import java.util.Map;

import static gpk.template.config.TemplateConnectorConfig.GREETING;
import static gpk.template.config.TemplateConnectorConfig.ID;

@Slf4j
public class TemplateSinkTask extends SinkTask {

    private TemplateConnectorConfig config;

    @Override
    public void initialize(SinkTaskContext context) {
        super.initialize(context);

        try {
            config = new TemplateConnectorConfig(context.configs());

            log.info("config values :");
            config.values().forEach((k, v) -> log.info("k : {}, v : {}", k, v));
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start sink connector due to configuration error", e);
        }
    }

    @Override
    public void start(Map<String, String> props) {}

    @Override
    public void put(Collection<SinkRecord> records) {
        log.info("{}, my id is {}", config.getString(GREETING), config.getInt(ID));
        records.forEach(r -> log.info("received record - {} : {}", r.key(), r.value()));
    }

    @Override
    public void stop() {}

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

}
