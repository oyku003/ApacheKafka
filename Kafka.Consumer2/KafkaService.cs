using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Consumer2
{
    public class KafkaService
    {
        internal async Task ConsumeSimpleMessageWithNullKey(string topicName)
        {

            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "case-1-group-2",
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
        }
}
