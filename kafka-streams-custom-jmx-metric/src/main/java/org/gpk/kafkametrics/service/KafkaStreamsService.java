package org.gpk.kafkametrics.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class KafkaStreamsService {
    final private AdminClient kafkaAdminClient;

    public void waitForTopic(String topic) {
        if (!topicExists(topic)) {
            log.info("waiting for topic {}...", topic);
        }
        try {
            while(!topicExists(topic)) {
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private boolean topicExists(String topic) {
        try {
            return kafkaAdminClient.listTopics().names().get().contains(topic);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return false;
        }
    }


}
