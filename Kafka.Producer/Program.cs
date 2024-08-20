// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer;

Console.WriteLine("Kafka Producer");

var kafkaService = new KafkaService();
//await kafkaService.CreateTopicForPartionName("topic2");


//await kafkaService.CreateTopic("topic3");
//await kafkaService.SendSimpleMessageWithNullKey("topic3");

//await kafkaService.CreateTopic("topic4");
//await kafkaService.SendSimpleMessageWithIntKey("topic4");

//await kafkaService.CreateTopic("topic4.1");
//await kafkaService.SendComplexMessageWithIntKey("topic4.1");

//await kafkaService.CreateTopic("topic5");
//await kafkaService.SendComplexMessageWithIntKeyAndHeader("topic5");

//await kafkaService.CreateTopic("topic6");
//await kafkaService.SendComplexMessageWithComplexKey("topic6");

//await kafkaService.CreateTopic("topic7");
//await kafkaService.SendMessageWithTimeStamp("topic7");

//await kafkaService.CreateTopicForPartionName("topic8");
//await kafkaService.SendMessageWithPartionName("topic8");


//for ack

//var topicName = "ack-topic";
//await kafkaService.CreateTopic(topicName);
//await kafkaService.SendMessageWithAck(topicName);

//for retenetion
//var topicName = "retention-topic";
//await kafkaService.CreateTopicForRetention(topicName);
//await kafkaService.SendMessageWithRetention(topicName);

//for cluster
//var topicName = "cluster-topic";
//await kafkaService.CreateTopicWithCluster(topicName);
//await kafkaService.SendMessageWithCluster(topicName);

//for retry
//var topicName = "retry-topic";
//await kafkaService.CreateTopicWithRetry(topicName);
//await kafkaService.SendMessageWithRetry(topicName);
//Console.WriteLine("Mesajlar gönderilmiştir.");