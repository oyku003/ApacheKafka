namespace Kafka.Consumer.Event
{
    internal record OrderCreatedEvent
    {
        public string OrderCode { get; init; } = default!;
        public decimal TotalPrice { get; init; }
        public int UserId { get; init; }
    }
}
