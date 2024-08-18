using Order.API.Services;
using Shared.Events.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddSingleton<IBus, Bus>(sp =>
{
    var logger = sp.GetService<ILogger<Bus>>();
    var bus = new Bus(builder.Configuration, logger!);
    bus.CreateTopicOrQueue([BusConsts.OrderCreatedEventTopicName]);

    return bus;
});
builder.Services.AddScoped<OrderService>();
var app = builder.Build();

//await app.CreateTopicOrQueues();//Ibus yukarda singleton yerine uyg ayaga kalkarken topic olu?ssun dersek bu extension? acabilirz

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
