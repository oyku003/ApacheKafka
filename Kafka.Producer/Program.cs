// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer;

Console.WriteLine("Kafka Producer");

var kafkaService = new KafkaService();
//await kafkaService.CreateTopic("case-6-topic");
//await kafkaService.CreateTopicForPartionName("case-7-topic");



//await kafkaService.SendSimpleMessageWithNullKey("case-1.1-topic");
//await kafkaService.SendSimpleMessageWithIntKey("case-2-topic");
//await kafkaService.SendComplexMessageWithIntKeyAndHeader("case-4-topic");
//await kafkaService.SendComplexMessageWithComplexKey("case-5-topic");
//await kafkaService.SendMessageWithTimeStamp("case-6-topic");
//await kafkaService.SendMessageWithPartionName("case-7-topic");


//for ack

//var topicName = "ack-topic";
//await kafkaService.CreateTopic(topicName);
//await kafkaService.SendMessageWithAck(topicName);

//for retenetion
//var topicName = "retention-topic";
//await kafkaService.CreateTopicForRetention(topicName);
//await kafkaService.SendMessageWithAck(topicName);

//for cluster
//var topicName = "mycluster3-topic";
//await kafkaService.CreateTopicWithCluster(topicName);
//await kafkaService.SendMessageWithCluster(topicName);

//for retry
var topicName = "retry-topic4";
await kafkaService.CreateTopicWithRetry(topicName);
await kafkaService.SendMessageWithRetryToCluster(topicName);
Console.WriteLine("Mesajlar gönderilmiştir.");