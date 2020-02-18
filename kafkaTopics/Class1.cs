using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace kafkaTopics
{
    public  class KafkaTopic
    {
        private ILogger _logger;

        public KafkaTopic(ILogger<KafkaTopic> logger)
        {
            _logger = logger;
        }
        public async Task CreateSingleTopic(AdminClientConfig adminConfig, TopicSpecification topicSpecs )
        {
            _logger.LogInformation(" Entered  CreateSingleTopic() with adminConfig : {0} , TopicSpecification : {1} ", adminConfig, topicSpecs);
            using (var adminclient = new AdminClientBuilder(adminConfig).Build())
            {
                try
                {

                    await adminclient.CreateTopicsAsync(new TopicSpecification[] { new TopicSpecification { Name = "topic", NumPartitions = 2, ReplicationFactor = 1 } });
                }
                catch (CreateTopicsException e)
                {
                    e.Results.ForEach(r => Console.WriteLine(r.Error.IsError ? "had error" : r.Error.Reason));
                    _logger.LogError("CreateTopicsException occured at CreateSingleTopic() : {0}", e);
                }
                catch (Exception ex)
                {
                    _logger.LogError("Exception occured at CreateSingleTopic() : {0}", ex);

                }
            }
               

        }

        public async Task CreateMultipleTpics(IEnumerable<BatchTopicConfiguration> topicBatchConfig)
        {
            _logger.LogInformation(" Entered  CreateMultipleTpics()  : {0} ", DateTime.UtcNow);
            foreach (var topicConfig in topicBatchConfig)
            {
                using (var adminclient = new AdminClientBuilder(topicConfig.adminClientConfig).Build())
                {
                    try
                    {
                        await adminclient.CreateTopicsAsync(new TopicSpecification[] { topicConfig.topicSpecs });
                    }
                    catch (CreateTopicsException e)
                    {
                        e.Results.ForEach(r => Console.WriteLine(r.Error.IsError ? "had error" : r.Error.Reason));
                        _logger.LogError("CreateTopicsException occured at CreateMultipleTpics() : {0}", e);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError("Exception occured at CreateMultipleTpics() : {0}", ex);

                    }
                }


            }

        }

        //public KafkaTopic()
        //{
        //    using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _config.GetValue<string>("KAFKA_BOOTSTRAP_SERVERS") }).Build())
        //    {
        //        var newTopic = new TopicSpecification
        //        {
        //            Name = "TEST_TOPIC",
        //            NumPartitions = 1,
        //            ReplicationFactor = 3 //but I have only 1 broker
        //        };
        //        await Task.FromResult(adminClient.CreateTopicsAsync(new[] { newTopic }));


               



        //        TopicSpecification topicx = new TopicSpecification()
        //        {
        //            Name = "test",
        //            NumPartitions = 1,
        //            ReplicationFactor = 1,
        //            Configs = new Dictionary<string, string> { { "retention.ms", "86400000" } }
        //        };
        //    }

        //}
    }
}
