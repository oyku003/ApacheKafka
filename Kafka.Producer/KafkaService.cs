using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer.Event;
using System;
using System.Diagnostics;
using System.Reflection;
using System.Text;

namespace Kafka.Producer
{
    internal class KafkaService
    {
        private const string _topicName = "mytopic";
        internal async Task CreateTopic(string topicName)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:9094"//3 brokerımız olsaydı 3 tane olcaktı.

            }).Build();

            try
            {
                var configs = new Dictionary<string, string>()
                {
                    {"message.timestamp.type","LogAppendTime"}//kafka confluentten öğrendik
                };

                await adminClient.CreateTopicsAsync(new[]
                {
            new TopicSpecification(){Name=topicName, NumPartitions=3, ReplicationFactor=1}
        });//tek broker old için replikaları olamaz.

                Console.WriteLine($"Topic {topicName} oluştu");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        internal async Task CreateTopicForPartionName(string topicName)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:9094"//3 brokerımız olsaydı 3 tane olcaktı.

            }).Build();

            try
            {
                await adminClient.CreateTopicsAsync(new[]
                {
            new TopicSpecification(){Name=topicName, NumPartitions=6, ReplicationFactor=1}
        });//tek broker old için replikaları olamaz.

                Console.WriteLine($"Topic {topicName} oluştu");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        internal async Task CreateTopicForRetention(string topicName)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:9094"//3 brokerımız olsaydı 3 tane olcaktı.

            }).Build();

            try
            {
                TimeSpan day30span = TimeSpan.FromDays(30);
                var configs = new Dictionary<string, string>()
                {
                    //{"retention.bytes", "10000" }//default =-1 (yani sınırlama yok), partion boyutunu simgeler.
                    // {"retention.ms","-1" }//default degerler confluentte var.-1: ömürboyu kafkada kalır
                    {"retention.ms", day30span.TotalMicroseconds.ToString() }
                };

                await adminClient.CreateTopicsAsync(new[]
                {
            new TopicSpecification(){Name=topicName, NumPartitions=6, ReplicationFactor=1, Configs=configs}
        });//tek broker old için replikaları olamaz.

                Console.WriteLine($"Topic {topicName} oluştu");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        internal async Task CreateTopicWithCluster(string topicName)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:7000,localhost:7001,localhost:7002"

            }).Build();

            try
            {
                await adminClient.CreateTopicsAsync(new[]
                {
            new TopicSpecification(){Name=topicName, NumPartitions=6, ReplicationFactor=3}//6 partionlı 3 brokerlı sistem
        });

                Console.WriteLine($"Topic {topicName} oluştu");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        internal async Task CreateTopicWithRetry(string topicName)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:7000,localhost:7001,localhost:7002"// 3 broker

            }).Build();

            try
            {
                var configs = new Dictionary<string, string>()
                {
                    {"min.insync.replicas","3" }//defaultu aslında 1 .3 oldugu için 1 lider 2 replikaya hepsine mutlakakaydedilmesi lazım
                    //gönderdiğim mesaj mutlaka 3 brokera da 1 lider 2 replicaya kaydolur. eger bunu 1 verip ack:all yapsaydık bile yine de 1 brokera gidecekti sadece, replikalara gitmeyecekti.
                };
                await adminClient.CreateTopicsAsync(new[]
                {
            new TopicSpecification(){Name=topicName, NumPartitions=6, ReplicationFactor=3, Configs=configs}//replicationFactor=3 demek 1 lider 2  replika demek. =4 olsaydı 1 lider 3 replika olacak anlamına gelirdi.
        });//tek broker old için replikaları olamaz.

                Console.WriteLine($"Topic {topicName} oluştu");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        internal async Task SendSimpleMessageWithNullKey(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<Null, string>(config).Build();//value typelar için ekstra serilize etmemize gerek yok library kendi yapıyor

            foreach (var item in Enumerable.Range(1, 10))
            {
                var result = await producer.ProduceAsync(topicName, new Message<Null, string>()
                {
                    Value = $"Message(case-1) {item}"

                });

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    await Console.Out.WriteLineAsync($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                await Console.Out.WriteLineAsync("--------------------------------");
                await Task.Delay(10);
            }
        }

        internal async Task SendSimpleMessageWithIntKey(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<int, string>(config).Build();//value typelar için ekstra serilize etmemize gerek yok library kendi yapıyor

            foreach (var item in Enumerable.Range(1, 100))
            {
                var result = await producer.ProduceAsync(topicName, new Message<int, string>()
                {
                    Key=item,
                    Value = $"Message(case-1) {item}"

                });

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    await Console.Out.WriteLineAsync($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                await Console.Out.WriteLineAsync("--------------------------------");
                await Task.Delay(10);
            }

        }

        internal async Task SendComplexMessageWithIntKey(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config)
                .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())//kafka bytearray bekler
                .Build();//value typelar için ekstra serilize etmemize gerek yok library kendi yapıyor

            foreach (var item in Enumerable.Range(1, 100))
            {

                var orderCreatedEvent = new OrderCreatedEvent
                {
                    OrderCode = Guid.NewGuid().ToString(),
                    TotalPrice = item * 100,
                    UserId = item

                };
                var result = await producer.ProduceAsync(topicName, new Message<int, OrderCreatedEvent>()
                {
                    Key = item,
                    Value = orderCreatedEvent

                });

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    await Console.Out.WriteLineAsync($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                await Console.Out.WriteLineAsync("--------------------------------");
                await Task.Delay(10);
            }

        }

        internal async Task SendComplexMessageWithIntKeyAndHeader(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config)
                .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())//kafka bytearray bekler
                .Build();//value typelar için ekstra serilize etmemize gerek yok library kendi yapıyor

            foreach (var item in Enumerable.Range(1, 3))
            {

                var orderCreatedEvent = new OrderCreatedEvent
                {
                    OrderCode = Guid.NewGuid().ToString(),
                    TotalPrice = item * 100,
                    UserId = item

                };
                var header =new Headers();
                header.Add(key: "correlation_id", val: Encoding.UTF8.GetBytes("123"));
                header.Add(key: "version", val: Encoding.UTF8.GetBytes("v1"));

                var result = await producer.ProduceAsync(topicName, new Message<int, OrderCreatedEvent>()
                {
                    Key = item,
                    Value = orderCreatedEvent,
                    Headers =header
                });

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    await Console.Out.WriteLineAsync($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                await Console.Out.WriteLineAsync("--------------------------------");
                await Task.Delay(10);
            }

        }

        internal async Task SendComplexMessageWithComplexKey(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<MessageKey, OrderCreatedEvent>(config)
                .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())//kafka bytearray bekler
                .SetKeySerializer(new CustomKeySerializer<MessageKey>())
                .Build();//value typelar için ekstra serilize etmemize gerek yok library kendi yapıyor

            foreach (var item in Enumerable.Range(1, 3))
            {

                var orderCreatedEvent = new OrderCreatedEvent
                {
                    OrderCode = Guid.NewGuid().ToString(),
                    TotalPrice = item * 100,
                    UserId = item

                };

                var result = await producer.ProduceAsync(topicName, new Message<MessageKey, OrderCreatedEvent>()
                {
                    Key = new MessageKey { Key1="key1 value", Key2="key2 value" },
                    Value = orderCreatedEvent
                });

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    await Console.Out.WriteLineAsync($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                await Console.Out.WriteLineAsync("--------------------------------");
                await Task.Delay(10);
            }

        }

        internal async Task SendMessageWithTimeStamp(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<MessageKey, OrderCreatedEvent>(config)
                .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())//kafka bytearray bekler
                .SetKeySerializer(new CustomKeySerializer<MessageKey>())
                .Build();//value typelar için ekstra serilize etmemize gerek yok library kendi yapıyor

            foreach (var item in Enumerable.Range(1, 3))
            {

                var orderCreatedEvent = new OrderCreatedEvent
                {
                    OrderCode = Guid.NewGuid().ToString(),
                    TotalPrice = item * 100,
                    UserId = item

                };

                var result = await producer.ProduceAsync(topicName, new Message<MessageKey, OrderCreatedEvent>()
                {
                    Key = new MessageKey { Key1 = "key1 value", Key2 = "key2 value" },
                    Value = orderCreatedEvent,
                   // Timestamp=new Timestamp(new DateTime) kafka zaten default koyuyor spesifik evrmiyosak gerek yok
                });

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    await Console.Out.WriteLineAsync($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                await Console.Out.WriteLineAsync("--------------------------------");
                await Task.Delay(10);
            }

        }

        internal async Task SendMessageWithPartionName(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<Null, string>(config)
                .Build();

            foreach (var item in Enumerable.Range(1, 10))
            {


                var topicParititon = new TopicPartition(topicName, new Partition(2));
                var message = new Message<Null, string>() { Value = $"Mesaj {item}" };
                var result = await producer.ProduceAsync(topicParititon, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    await Console.Out.WriteLineAsync($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                await Console.Out.WriteLineAsync("--------------------------------");
                await Task.Delay(10);
            }

        }

        internal async Task SendMessageWithAck(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094", Acks= Acks.All };//none//lead

            using var producer = new ProducerBuilder<Null, string>(config)
                .Build();

            foreach (var item in Enumerable.Range(1, 10))
            {


                var topicParititon = new TopicPartition(topicName, new Partition(2));
                var message = new Message<Null, string>() { Value = $"Mesaj {item}" };
                var result = await producer.ProduceAsync(topicParititon, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    await Console.Out.WriteLineAsync($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                await Console.Out.WriteLineAsync("--------------------------------");
                await Task.Delay(10);
            }

        }

        internal async Task SendMessageWithCluster(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:7000,localhost:7001,localhost:7002", Acks = Acks.All };//none//lead
            //7000 olan ayakta degilse hata verir. ama  3 unu yazıp 7000 down yaparsak diğerlerinden bilgileri bulup bağlanır.
            using var producer = new ProducerBuilder<Null, string>(config)
                .Build();

            foreach (var item in Enumerable.Range(1, 10))
            {


                //var topicParititon = new TopicPartition(topicName);
                var message = new Message<Null, string>() { Value = $"Mesaj {item}" };
                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    await Console.Out.WriteLineAsync($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                await Console.Out.WriteLineAsync("--------------------------------");
                await Task.Delay(10);
            }

        }

        internal async Task SendMessageWithRetention(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094", Acks = Acks.All };//none//lead

            using var producer = new ProducerBuilder<Null, string>(config)
                .Build();

            foreach (var item in Enumerable.Range(1, 10))
            {


                var topicParititon = new TopicPartition(topicName, new Partition(2));
                var message = new Message<Null, string>() { Value = $"Mesaj {item}" };
                var result = await producer.ProduceAsync(topicParititon, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    await Console.Out.WriteLineAsync($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                await Console.Out.WriteLineAsync("--------------------------------");
                await Task.Delay(10);
            }

        }

        internal async Task SendMessageWithRetry(string topicName)
        {

            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:7000",
                Acks = Acks.All,
                MessageTimeoutMs = 6000,
                MessageSendMaxRetries = 2,
                RetryBackoffMs = 2000,//her retry arasındaki süre 
                RetryBackoffMaxMs = 2000,//her retry arasındaki max süre ,üstüne kesinlikle cıkmasın,


            };


            using var producer = new ProducerBuilder<Null, string>(config).Build();


            var message = new Message<Null, string>()
            {
                Value = $"Mesaj 1"
            };


            var result = await producer.ProduceAsync(topicName, message);


            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("-----------------------------------");
            await Task.Delay(10);
        }
        internal async Task SendMessageWithRetryToCluster(string topicName)
        {
            Task.Run(async () =>
            {
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();
                while (true)
                {
                    TimeSpan timeSpan = TimeSpan.FromSeconds(Convert.ToInt32(stopwatch.Elapsed.TotalSeconds));
                    Console.Write(timeSpan.ToString("c"));
                    Console.Write('\r');


                    await Task.Delay(1000);
                }
            });

            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:7000,localhost:7001,localhost:7002",
                Acks = Acks.All,
                //MessageTimeoutMs = 6000,
                MessageSendMaxRetries = 1,
                RetryBackoffMs = 2000,//her retry arasındaki süre 
                RetryBackoffMaxMs = 2000,//her retry arasındaki max süre ,üstüne kesinlikle cıkmasın,
               
                
            };


            using var producer = new ProducerBuilder<Null, string>(config).Build();


            var message = new Message<Null, string>()
            {
                Value = $"Mesaj 1"
            };


            var result = await producer.ProduceAsync(topicName, message);


            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("-----------------------------------");
            await Task.Delay(10);
        }



    }
}
