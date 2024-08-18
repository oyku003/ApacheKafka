using Order.API.Dtos;
using Shared.Events.Events;
using Shared.Events.Services;

namespace Order.API.Services
{
    public class OrderService(IBus bus)
    {
        public async Task<bool> Create(OrderCreatedRequestDto orderCreatedRequestDto)
        {
            //save

            var orderCode = Guid.NewGuid().ToString();
            var orderCreatedEvent = new OrderCreatedEvent(orderCode, UserId: orderCreatedRequestDto.UserId, TotalPrice: orderCreatedRequestDto.TotalPrice);

            return await bus.Publish(orderCode, orderCreatedEvent, BusConsts.OrderCreatedEventTopicName);
        }
    }
}
