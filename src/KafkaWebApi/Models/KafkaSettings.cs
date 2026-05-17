namespace KafkaWebApi.Models;

public class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string OrdersTopic { get; set; } = "orders";
    public string DeadLetterTopic { get; set; } = "orders-dlt";
    public string ConsumerGroupId { get; set; } = "orders-consumer-group";
    public string NotificationBaseUrl { get; set; } = "http://localhost:5001";
}
