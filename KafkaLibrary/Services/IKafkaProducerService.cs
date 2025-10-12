namespace KafkaLibrary.Services;

public interface IKafkaProducerService<in TKey, in TValue>
{
    Task<bool> ProduceAsync(TKey key, TValue value, string? topic = null);
    Task<bool> ProduceAsync(TValue value, string? topic = null);
}