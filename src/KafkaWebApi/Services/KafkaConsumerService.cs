using System.Text.Json;
using Confluent.Kafka;
using KafkaWebApi.Models;
using Microsoft.Extensions.Options;

namespace KafkaWebApi.Services;

public class KafkaConsumerService : BackgroundService
{
    private readonly KafkaSettings _settings;
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly OrderStore _orderStore;
    private readonly IKafkaProducerService _producer;
    private readonly IServiceScopeFactory _scopeFactory;

    public KafkaConsumerService(
        IOptions<KafkaSettings> settings,
        ILogger<KafkaConsumerService> logger,
        OrderStore orderStore,
        IKafkaProducerService producer,
        IServiceScopeFactory scopeFactory)
    {
        _settings = settings.Value;
        _logger = logger;
        _orderStore = orderStore;
        _producer = producer;
        _scopeFactory = scopeFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Kafka consumer starting...");

        await Task.Yield();

        var config = new ConsumerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            GroupId = _settings.ConsumerGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(_settings.OrdersTopic);

        _logger.LogInformation("Subscribed to topic {Topic}", _settings.OrdersTopic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(stoppingToken);

                    if (result?.Message?.Value is null)
                        continue;

                    _logger.LogInformation(
                        "Consumed message from {Topic} [{Partition}] at offset {Offset}",
                        result.Topic, result.Partition.Value, result.Offset.Value);

                    var processed = await ProcessMessageAsync(result, stoppingToken);

                    if (processed)
                    {
                        consumer.StoreOffset(result);
                        consumer.Commit(result);

                        _logger.LogDebug("Offset committed for [{Partition}] at {Offset}",
                            result.Partition.Value, result.Offset.Value);
                    }
                    else
                    {
                        _logger.LogWarning(
                            "Skipping offset commit for [{Partition}] at {Offset} due to processing failure",
                            result.Partition.Value, result.Offset.Value);
                    }
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Error consuming message");
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Kafka consumer stopping gracefully...");
        }
        finally
        {
            try
            {
                consumer.Commit();
                _logger.LogInformation("Final offset commit completed");
            }
            catch (KafkaException ex)
            {
                _logger.LogWarning(ex, "Error during final offset commit");
            }

            consumer.Close();
            _logger.LogInformation("Kafka consumer stopped");
        }
    }

    private async Task<bool> ProcessMessageAsync(ConsumeResult<string, string> result, CancellationToken cancellationToken)
    {
        try
        {
            var order = JsonSerializer.Deserialize<Order>(result.Message.Value);
            if (order is null)
            {
                _logger.LogWarning("Failed to deserialize message, sending to DLT");
                return await SendToDeadLetterAsync(result, "Deserialization returned null");
            }

            order.Status = "Processed";
            _orderStore.AddOrUpdate(order);

            using var scope = _scopeFactory.CreateScope();
            var notificationService = scope.ServiceProvider.GetRequiredService<IOrderNotificationService>();
            await notificationService.NotifyOrderProcessedAsync(order, cancellationToken);

            _logger.LogInformation("Order {OrderId} processed successfully", order.Id);
            return true;
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Failed to deserialize message, sending to DLT");
            return await SendToDeadLetterAsync(result, $"Deserialization error: {ex.Message}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing message, sending to DLT");
            return await SendToDeadLetterAsync(result, $"Processing error: {ex.Message}");
        }
    }

    private async Task<bool> SendToDeadLetterAsync(ConsumeResult<string, string> result, string reason)
    {
        try
        {
            var dltMessage = JsonSerializer.Serialize(new
            {
                originalTopic = result.Topic,
                originalPartition = result.Partition.Value,
                originalOffset = result.Offset.Value,
                originalKey = result.Message.Key,
                originalValue = result.Message.Value,
                reason,
                timestamp = DateTime.UtcNow
            });

            await _producer.ProduceAsync(
                _settings.DeadLetterTopic,
                result.Message.Key ?? "unknown",
                dltMessage);

            _logger.LogWarning("Message sent to dead-letter topic {Topic}: {Reason}",
                _settings.DeadLetterTopic, reason);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send message to dead-letter topic");
            return false;
        }
    }
}
