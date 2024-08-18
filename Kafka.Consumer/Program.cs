// See https://aka.ms/new-console-template for more information
using Kafka.Consumer;

Console.WriteLine("Kafka Consumer 1");

var topicName = "mycluster2-topic";
var kafkaService = new KafkaService();

//await kafkaService.ConsumeSimpleMessageWithNullKey(topicName);
//await kafkaService.ConsumeSimpleMessageWithIntKey(topicName);
//await kafkaService.ConsumeComplexMessageWithIntKey(topicName);
//await kafkaService.ConsumeComplexMessageWithIntKeyAndHeader(topicName);
//await kafkaService.ConsumeComplexMessageWithComplexKey(topicName);
//await kafkaService.ConsumeMessageWithTimeStamp(topicName);
//await kafkaService.ConsumeMessageFromSpecificPartition(topicName);
//await kafkaService.ConsumeMessageFromSpecificPartitionOffset(topicName);
//await kafkaService.ConsumeMessageFromSpecificPartitionOffsetForAck(topicName);
await kafkaService.ConsumeMessageFromCluster(topicName);

Console.ReadLine();


