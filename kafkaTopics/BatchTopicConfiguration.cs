using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafkaTopics
{
   public  class BatchTopicConfiguration
    {
        public AdminClientConfig adminClientConfig { get; set; } = new AdminClientConfig();
        public TopicSpecification topicSpecs { get; set; } = new TopicSpecification();
    }
}
