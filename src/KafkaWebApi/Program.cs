using KafkaWebApi.Models;
using KafkaWebApi.Resilience;
using KafkaWebApi.Services;

var builder = WebApplication.CreateBuilder(args);

var kafkaSettings = builder.Configuration.GetSection("Kafka");
builder.Services.Configure<KafkaSettings>(kafkaSettings);

builder.Services.AddSingleton<OrderStore>();
builder.Services.AddSingleton<IKafkaProducerService, KafkaProducerService>();
builder.Services.AddHostedService<KafkaConsumerService>();

var notificationBaseUrl = kafkaSettings.GetValue<string>("NotificationBaseUrl") ?? "http://localhost:5001";

builder.Services.AddHttpClient<IOrderNotificationService, OrderNotificationService>(client =>
{
    client.BaseAddress = new Uri(notificationBaseUrl);
    client.DefaultRequestHeaders.Add("Accept", "application/json");
    client.Timeout = TimeSpan.FromSeconds(15);
})
.AddPolicyHandler(HttpClientPolicies.GetRetryPolicy())
.AddPolicyHandler(HttpClientPolicies.GetCircuitBreakerPolicy())
.AddPolicyHandler(HttpClientPolicies.GetTimeoutPolicy());

builder.Services.AddHealthChecks()
    .AddCheck<KafkaHealthCheck>("kafka", tags: new[] { "ready", "kafka" });

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.MapControllers();
app.MapHealthChecks("/health");

app.Run();
