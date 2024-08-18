using Shared.Events.Services;

namespace Order.API.Services
{
    public static class BusExt
    {
        public static async Task CreateTopicOrQueues(this WebApplication app)
        {
            using (var scope = app.Services.CreateScope())
            {
                var bus = scope.ServiceProvider.GetRequiredService<IBus>();
                await bus.CreateTopicOrQueue([BusConsts.OrderCreatedEventTopicName]);
            };
        }
    }
}
