using System.Collections.Concurrent;
using KafkaWebApi.Models;

namespace KafkaWebApi.Services;

public class OrderStore
{
    private readonly ConcurrentDictionary<Guid, Order> _orders = new();

    public void AddOrUpdate(Order order)
    {
        _orders.AddOrUpdate(order.Id, order, (_, _) => order);
    }

    public Order? GetById(Guid id)
    {
        _orders.TryGetValue(id, out var order);
        return order;
    }

    public IReadOnlyList<Order> GetAll()
    {
        return _orders.Values.ToList().AsReadOnly();
    }
}
