using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Shared.Events;

namespace Stock.API.Services
{
    public class Bus(IConfiguration configuration, ILogger<Bus> logger ) :IBus
    {
        public ConsumerConfig GetConsumerConfig(string groupId)
        {
            return new ConsumerConfig()
            {
                BootstrapServers = configuration.GetSection("BusSettings").GetSection("Kafka")["BootstrapServers"],
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true//for ack//arka tarafta kafka background job çalıştırıp otomatik olarak ack bilgilerini iletir, false deseydik ack bilgisini biz gönderecegiz olacaktı.
            };
        }

        
    }
}

