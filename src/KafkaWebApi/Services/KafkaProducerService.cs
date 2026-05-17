using System.Text.Json;
using Confluent.Kafka;
using KafkaWebApi.Models;
using Microsoft.Extensions.Options;

namespace KafkaWebApi.Services;

public interface IKafkaProducerService
{
    Task<DeliveryResult<string, string>> ProduceAsync(string topic, string key, object message);
}

public class KafkaProducerService : IKafkaProducerService, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducerService> _logger;

    public KafkaProducerService(IOptions<KafkaSettings> settings, ILogger<KafkaProducerService> logger)
    {
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = settings.Value.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageSendMaxRetries = 3,
            RetryBackoffMs = 1000,
            LingerMs = 5,
            BatchSize = 16384
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task<DeliveryResult<string, string>> ProduceAsync(string topic, string key, object message)
    {
        var json = JsonSerializer.Serialize(message);

        _logger.LogInformation("Producing message to topic {Topic} with key {Key}", topic, key);

        var result = await _producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = key,
            Value = json
        });

        _logger.LogInformation(
            "Message delivered to {Topic} [{Partition}] at offset {Offset}",
            result.Topic, result.Partition.Value, result.Offset.Value);

        return result;
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
