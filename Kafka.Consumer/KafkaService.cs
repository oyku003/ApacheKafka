using Confluent.Kafka;
using Kafka.Consumer.Event;
using System.Text;

namespace Kafka.Consumer
{
    public class KafkaService
    {
        internal async Task ConsumeSimpleMessageWithNullKey(string topicName)
        {

            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Null, string>(config).Build();

            consumer.Subscribe(topicName);

            while(true)
            {
                var consumeResult= consumer.Consume(millisecondsTimeout:5000);//metot her seferinde kafkaya gitmez. hepsini alır memorye alır.memorydeki boşalınca tekrar gider kafkaya!!!. Burası bloklayıcı bir işlem. timeout vererek kafkada bekleyecegi süreyi verebiliriz.

                if(consumeResult != null)
                {
                    await Console.Out.WriteLineAsync($"Gelen mesaj: {consumeResult.Message.Value}");

                }
                await Task.Delay(500);
            }
        }

        internal async Task ConsumeSimpleMessageWithIntKey(string topicName)
        {

            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<int, string>(config).Build();

            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(millisecondsTimeout: 5000);//metot her seferinde kafkaya gitmez. hepsini alır memorye alır.memorydeki boşalınca tekrar gider kafkaya!!!. Burası bloklayıcı bir işlem. timeout vererek kafkada bekleyecegi süreyi verebiliriz.

                if (consumeResult != null)
                {
                    await Console.Out.WriteLineAsync($"Gelen mesaj: Key:{consumeResult.Message.Key} Value= {consumeResult.Message.Value}");

                }
                await Task.Delay(10);
            }
        }

        internal async Task ConsumeComplexMessageWithIntKey(string topicName)
        {

            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config)
                .SetValueDeserializer(new CustomKeyDeserializer<OrderCreatedEvent>())
                .Build();

            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(millisecondsTimeout: 5000);//metot her seferinde kafkaya gitmez. hepsini alır memorye alır.memorydeki boşalınca tekrar gider kafkaya!!!. Burası bloklayıcı bir işlem. timeout vererek kafkada bekleyecegi süreyi verebiliriz.

                if (consumeResult != null)
                {
                    await Console.Out.WriteLineAsync($"Gelen mesaj: Key:{consumeResult.Message.Key} Value= {consumeResult.Message.Value.UserId}-{consumeResult.Message.Value.OrderCode}-{consumeResult.Message.Value.TotalPrice}");

                }
                await Task.Delay(10);
            }
        }

        internal async Task ConsumeComplexMessageWithIntKeyAndHeader(string topicName)
        {

            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config)
                .SetValueDeserializer(new CustomKeyDeserializer<OrderCreatedEvent>())
                .Build();

            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(millisecondsTimeout: 5000);//metot her seferinde kafkaya gitmez. hepsini alır memorye alır.memorydeki boşalınca tekrar gider kafkaya!!!. Burası bloklayıcı bir işlem. timeout vererek kafkada bekleyecegi süreyi verebiliriz.

                if (consumeResult != null)
                {
                    var correlationId = Encoding.UTF8.GetString(consumeResult.Message.Headers.GetLastBytes("correlation_id"));
                    var version =Encoding.UTF8.GetString(consumeResult.Message.Headers.GetLastBytes("version"));

                    Console.WriteLine($"Headers: correlation_id:{correlationId}, version:{version}");
                    await Console.Out.WriteLineAsync($"Gelen mesaj: Key:{consumeResult.Message.Key} Value= {consumeResult.Message.Value.UserId}-{consumeResult.Message.Value.OrderCode}-{consumeResult.Message.Value.TotalPrice}");

                }
                await Task.Delay(10);
            }
        }

        internal async Task ConsumeComplexMessageWithComplexKey(string topicName)
        {

            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<MessageKey, OrderCreatedEvent>(config)
                .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
                .SetKeyDeserializer(new CustomKeyDeserializer<MessageKey>())
                .Build();

            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(millisecondsTimeout: 5000);//metot her seferinde kafkaya gitmez. hepsini alır memorye alır.memorydeki boşalınca tekrar gider kafkaya!!!. Burası bloklayıcı bir işlem. timeout vererek kafkada bekleyecegi süreyi verebiliriz.

                if (consumeResult != null)
                {
                    var messageKey = consumeResult.Message.Key;
                    await Console.Out.WriteLineAsync($"Gelen mesaj: Key1:{messageKey.Key1}, Key2:{messageKey.Key2}, Value= {consumeResult.Message.Value.UserId}-{consumeResult.Message.Value.OrderCode}-{consumeResult.Message.Value.TotalPrice}");

                }
                await Task.Delay(10);
            }
        }

        internal async Task ConsumeMessageWithTimeStamp(string topicName)
        {

            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<MessageKey, OrderCreatedEvent>(config)
                .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
                .SetKeyDeserializer(new CustomKeyDeserializer<MessageKey>())
                .Build();

            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(millisecondsTimeout: 5000);//metot her seferinde kafkaya gitmez. hepsini alır memorye alır.memorydeki boşalınca tekrar gider kafkaya!!!. Burası bloklayıcı bir işlem. timeout vererek kafkada bekleyecegi süreyi verebiliriz.

                if (consumeResult != null)
                {
                    Console.WriteLine($"Message Timestamp: {consumeResult.Message.Timestamp.UtcDateTime}");//zamanla çalışırken datetimeoffset.now kullan utc vermesi lazım +3 çalışma

                }
                await Task.Delay(10);
            }
        }

        internal async Task ConsumeMessageFromSpecificPartition(string topicName)
        {

            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Null, string>(config)
                .Build();

            consumer.Assign(new TopicPartition(topicName, new Partition(2)));

            while (true)
            {
                var consumeResult = consumer.Consume(millisecondsTimeout: 5000);//metot her seferinde kafkaya gitmez. hepsini alır memorye alır.memorydeki boşalınca tekrar gider kafkaya!!!. Burası bloklayıcı bir işlem. timeout vererek kafkada bekleyecegi süreyi verebiliriz.

                if (consumeResult != null)
                {
                    Console.WriteLine($"Message Timestamp: {consumeResult.Message.Value}");

                }
                await Task.Delay(10);
            }
        }

        internal async Task ConsumeMessageFromSpecificPartitionOffset(string topicName)
        {

            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Null, string>(config)
                .Build();

            consumer.Assign(new TopicPartitionOffset(topicName, 2,4));//5den itibaren okumaya başlar

            while (true)
            {
                var consumeResult = consumer.Consume(millisecondsTimeout: 5000);//metot her seferinde kafkaya gitmez. hepsini alır memorye alır.memorydeki boşalınca tekrar gider kafkaya!!!. Burası bloklayıcı bir işlem. timeout vererek kafkada bekleyecegi süreyi verebiliriz.

                if (consumeResult != null)
                {
                    Console.WriteLine($"Message Timestamp: {consumeResult.Message.Value}");

                }
                await Task.Delay(10);
            }
        }

        internal async Task ConsumeMessageFromSpecificPartitionOffsetForAck(string topicName)
        {

            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true//for ack//arka tarafta kafka background job çalıştırıp otomatik olarak ack bilgilerini iletir, false deseydik ack bilgisini biz gönderecegiz olacaktı.
            };

            var consumer = new ConsumerBuilder<Null, string>(config)
                .Build();

            consumer.Assign(new TopicPartitionOffset(topicName, 2, 4));//5den itibaren okumaya başlar

            while (true)
            {
                var consumeResult = consumer.Consume(millisecondsTimeout: 5000);//metot her seferinde kafkaya gitmez. hepsini alır memorye alır.memorydeki boşalınca tekrar gider kafkaya!!!. Burası bloklayıcı bir işlem. timeout vererek kafkada bekleyecegi süreyi verebiliriz.

                if (consumeResult != null)
                {
                    Console.WriteLine($"Message Timestamp: {consumeResult.Message.Value}");
                    consumer.Commit(consumeResult);//for ack:false için, manuel ack iletim.

                }
                await Task.Delay(10);
            }
        }

        internal async Task ConsumeMessageFromCluster(string topicName)
        {

            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:7000,localhost:7001,localhost:7002",
                GroupId = "group-x",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true//for ack//arka tarafta kafka background job çalıştırıp otomatik olarak ack bilgilerini iletir, false deseydik ack bilgisini biz gönderecegiz olacaktı.
            };

            var consumer = new ConsumerBuilder<Null, string>(config)
                .Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(millisecondsTimeout: 5000);//metot her seferinde kafkaya gitmez. hepsini alır memorye alır.memorydeki boşalınca tekrar gider kafkaya!!!. Burası bloklayıcı bir işlem. timeout vererek kafkada bekleyecegi süreyi verebiliriz.

                if (consumeResult != null)
                {
                    Console.WriteLine($"Message Timestamp: {consumeResult.Message.Value}");
                    consumer.Commit(consumeResult);//for ack:false için, manuel ack iletim.

                }
                await Task.Delay(10);
            }
        }
    }
}
