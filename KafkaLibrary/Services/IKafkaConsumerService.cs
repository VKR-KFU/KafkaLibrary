namespace KafkaLibrary.Services;

public interface IKafkaConsumerService<TKey, TValue>
{
    Task<TValue?> ConsumeAsync(CancellationToken cancellationToken = default);
    void Subscribe(string topic);
    void Subscribe(IEnumerable<string> topics);
    Task<TValue?> ConsumeAsync(string topic, CancellationToken cancellationToken = default);
    void Commit();
}