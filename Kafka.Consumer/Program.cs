// See https://aka.ms/new-console-template for more information
using Kafka.Consumer;

Console.WriteLine("Kafka Consumer 1");

//var topicName = "case-6-topic";
var kafkaService = new KafkaService();

//await kafkaService.ConsumeSimpleMessageWithNullKey("topic3");
//await kafkaService.ConsumeSimpleMessageWithIntKey("topic4");
//await kafkaService.ConsumeComplexMessageWithIntKey("topic4.1");
//await kafkaService.ConsumeComplexMessageWithIntKeyAndHeader("topic5");
//await kafkaService.ConsumeComplexMessageWithComplexKey("topic6");
//await kafkaService.ConsumeMessageWithTimeStamp("topic7");
//await kafkaService.ConsumeMessageFromSpecificPartition("topic8");
//await kafkaService.ConsumeMessageFromSpecificPartitionOffset("topic8");
//await kafkaService.ConsumeMessageFromSpecificPartitionOffsetForAck("ack-topic");
//await kafkaService.ConsumeMessageFromCluster("cluster-topic");

Console.ReadLine();


