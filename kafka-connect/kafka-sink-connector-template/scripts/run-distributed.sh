#!/bin/bash

KAFKA_REST_URL=http://localhost:8083

curl -i -H "Content-Type: application/json" -XPOST $KAFKA_REST_URL/connectors -d @- <<EOF
{
  "name": "sink-connector-template",
  "config": {
    "connector.class": "TemplateSinkConnector",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "topics": "template_connector_input",
    "task.max": 1,
    "example.greeting": "hello",
    "example.id": 42
  }
}
EOF
