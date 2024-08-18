using Confluent.Kafka;
using Shared.Events;
using Shared.Events.Events;
using Shared.Events.Services;
using Stock.API.Services;

namespace Stock.API.BackgroundServices
{
    public class OrderCreatedEventConsumerBackgroundService(IBus bus) : BackgroundService
    {
        private IConsumer<string, OrderCreatedEvent>? _consumer;
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _consumer =new ConsumerBuilder<string, OrderCreatedEvent>(bus.GetConsumerConfig(BusConsts.OrderCreatedEventGroupId))
                .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
               .Build();
            _consumer.Subscribe(BusConsts.OrderCreatedEventTopicName);

            return base.StartAsync(cancellationToken);
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
          

            while (!stoppingToken.IsCancellationRequested)
            {
                var consumeResult = _consumer.Consume(millisecondsTimeout: 5000);//metot her seferinde kafkaya gitmez. hepsini alır memorye alır.memorydeki boşalınca tekrar gider kafkaya!!!. Burası bloklayıcı bir işlem. timeout vererek kafkada bekleyecegi süreyi verebiliriz.

                if (consumeResult != null)
                {
                    try
                    {
                        var orderCreatedEvent = consumeResult.Message.Value;

                        await Console.Out.WriteLineAsync($"userid: {orderCreatedEvent.UserId}, TotalPrice: {orderCreatedEvent.TotalPrice}, OrderCode: {orderCreatedEvent.OrderCode}");
                        _consumer.Commit(consumeResult);
                    }
                    catch (Exception e)
                    {
                        await Console.Out.WriteLineAsync(e.Message);
                        throw;
                    }
                    Console.WriteLine($"Message Timestamp: {consumeResult.Message.Value}");
                    _consumer.Commit(consumeResult);//for ack:false için, manuel ack ilettik.

                }

                await Task.Delay(10, stoppingToken);
            }
        }
    }
}
