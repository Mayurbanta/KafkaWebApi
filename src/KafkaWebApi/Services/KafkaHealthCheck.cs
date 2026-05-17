using Confluent.Kafka;
using KafkaWebApi.Models;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace KafkaWebApi.Services;

public class KafkaHealthCheck : IHealthCheck
{
    private readonly KafkaSettings _settings;
    private readonly ILogger<KafkaHealthCheck> _logger;

    public KafkaHealthCheck(IOptions<KafkaSettings> settings, ILogger<KafkaHealthCheck> logger)
    {
        _settings = settings.Value;
        _logger = logger;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = _settings.BootstrapServers
            };

            using var adminClient = new AdminClientBuilder(config).Build();
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));

            var brokerCount = metadata.Brokers.Count;
            var topicCount = metadata.Topics.Count;

            _logger.LogDebug("Kafka health check passed: {BrokerCount} brokers, {TopicCount} topics",
                brokerCount, topicCount);

            return await Task.FromResult(HealthCheckResult.Healthy(
                $"Kafka is healthy. Brokers: {brokerCount}, Topics: {topicCount}",
                new Dictionary<string, object>
                {
                    { "brokers", brokerCount },
                    { "topics", topicCount },
                    { "bootstrapServers", _settings.BootstrapServers }
                }));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Kafka health check failed");
            return await Task.FromResult(HealthCheckResult.Unhealthy(
                "Kafka is unhealthy", ex));
        }
    }
}
