namespace KafkaWebApi.Models;

public class Order
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string ProductName { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal Price { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public string Status { get; set; } = "Pending";
}

public class CreateOrderRequest
{
    [System.ComponentModel.DataAnnotations.Required]
    [System.ComponentModel.DataAnnotations.StringLength(200, MinimumLength = 1)]
    public string ProductName { get; set; } = string.Empty;

    [System.ComponentModel.DataAnnotations.Range(1, int.MaxValue, ErrorMessage = "Quantity must be at least 1")]
    public int Quantity { get; set; }

    [System.ComponentModel.DataAnnotations.Range(0.01, double.MaxValue, ErrorMessage = "Price must be greater than 0")]
    public decimal Price { get; set; }
}
