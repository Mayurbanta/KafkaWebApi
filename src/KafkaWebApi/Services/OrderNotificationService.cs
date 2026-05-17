using System.Text;
using System.Text.Json;
using KafkaWebApi.Models;

namespace KafkaWebApi.Services;

public interface IOrderNotificationService
{
    Task<bool> NotifyOrderProcessedAsync(Order order, CancellationToken cancellationToken = default);
}

public class OrderNotificationService : IOrderNotificationService
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<OrderNotificationService> _logger;

    public OrderNotificationService(HttpClient httpClient, ILogger<OrderNotificationService> logger)
    {
        _httpClient = httpClient;
        _logger = logger;
    }

    public async Task<bool> NotifyOrderProcessedAsync(Order order, CancellationToken cancellationToken = default)
    {
        try
        {
            var payload = JsonSerializer.Serialize(new
            {
                orderId = order.Id,
                productName = order.ProductName,
                status = order.Status,
                processedAt = DateTime.UtcNow
            });

            var content = new StringContent(payload, Encoding.UTF8, "application/json");

            _logger.LogInformation("Sending notification for order {OrderId}", order.Id);

            var response = await _httpClient.PostAsync("/notifications/orders", content, cancellationToken);
            response.EnsureSuccessStatusCode();

            _logger.LogInformation("Notification sent successfully for order {OrderId}", order.Id);
            return true;
        }
        catch (HttpRequestException ex)
        {
            _logger.LogError(ex, "Failed to send notification for order {OrderId}", order.Id);
            return false;
        }
        catch (TaskCanceledException ex)
        {
            _logger.LogWarning(ex, "Notification request timed out for order {OrderId}", order.Id);
            return false;
        }
    }
}
