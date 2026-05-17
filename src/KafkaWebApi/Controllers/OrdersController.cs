using KafkaWebApi.Models;
using KafkaWebApi.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace KafkaWebApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class OrdersController : ControllerBase
{
    private readonly IKafkaProducerService _producer;
    private readonly KafkaSettings _settings;
    private readonly OrderStore _orderStore;
    private readonly ILogger<OrdersController> _logger;

    public OrdersController(
        IKafkaProducerService producer,
        IOptions<KafkaSettings> settings,
        OrderStore orderStore,
        ILogger<OrdersController> logger)
    {
        _producer = producer;
        _settings = settings.Value;
        _orderStore = orderStore;
        _logger = logger;
    }

    [HttpPost]
    public async Task<IActionResult> CreateOrder([FromBody] CreateOrderRequest request)
    {
        var order = new Order
        {
            ProductName = request.ProductName,
            Quantity = request.Quantity,
            Price = request.Price
        };

        _orderStore.AddOrUpdate(order);

        await _producer.ProduceAsync(_settings.OrdersTopic, order.Id.ToString(), order);

        _logger.LogInformation("Order {OrderId} created and sent to Kafka", order.Id);

        return CreatedAtAction(nameof(GetOrder), new { id = order.Id }, order);
    }

    [HttpGet("{id:guid}")]
    public IActionResult GetOrder(Guid id)
    {
        var order = _orderStore.GetById(id);
        if (order is null)
            return NotFound();

        return Ok(order);
    }

    [HttpGet]
    public IActionResult GetAllOrders()
    {
        return Ok(_orderStore.GetAll());
    }
}
