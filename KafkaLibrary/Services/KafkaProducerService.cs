using Confluent.Kafka;
using KafkaLibrary.JsonSerializers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaLibrary.Services;

public class KafkaProducerService<TKey, TValue> : IKafkaProducerService<TKey, TValue>
{
    private readonly IProducer<TKey, TValue> _producer;
    private readonly KafkaSettings _kafkaSettings;
    private readonly ILogger<KafkaProducerService<TKey, TValue>> _logger;
    private bool _disposed = false;

    public KafkaProducerService(
        IOptions<KafkaSettings> kafkaSettings,
        ILogger<KafkaProducerService<TKey, TValue>> logger)
    {
        _kafkaSettings = kafkaSettings.Value;
        _logger = logger;

        if (string.IsNullOrEmpty(_kafkaSettings.Topic))
            throw new ArgumentNullException(nameof(_kafkaSettings.Topic));
        
        if (string.IsNullOrEmpty(_kafkaSettings.GroupId))
            throw new ArgumentNullException(nameof(_kafkaSettings.GroupId));
        
        if (string.IsNullOrEmpty(_kafkaSettings.BootstrapServers))
            throw new ArgumentNullException(nameof(_kafkaSettings.BootstrapServers));
        
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _kafkaSettings.BootstrapServers,
            ClientId = Environment.MachineName,
            Acks = Acks.All,
            MessageSendMaxRetries = 3,
            MessageTimeoutMs = 15000,
        };

        _producer = new ProducerBuilder<TKey, TValue>(producerConfig)
            .SetValueSerializer(new JsonSerializer<TValue>())
            .SetErrorHandler((_, error) => 
            {
                _logger.LogError("Kafka Producer Error: {Reason}", error.Reason);
            })
            .Build();
    }

    public async Task<bool> ProduceAsync(TKey key, TValue value, string? topic = null)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(KafkaProducerService<TKey, TValue>));

        var targetTopic = topic ?? _kafkaSettings.Topic;

        try
        {
            var result = await _producer.ProduceAsync(targetTopic, 
                new Message<TKey, TValue> { Key = key, Value = value });
            
            _logger.LogDebug("Message produced to {Topic}, partition {Partition}, offset {Offset}",
                targetTopic, result.Partition, result.Offset);
            
            return result.Status == PersistenceStatus.Persisted;
        }
        catch (ProduceException<TKey, TValue> ex)
        {
            _logger.LogError(ex, "Failed to deliver message to topic {Topic}", targetTopic);
            return false;
        }
    }

    public async Task<bool> ProduceAsync(TValue value, string? topic = null)
    {
        return await ProduceAsync(default!, value, topic);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _producer?.Flush(TimeSpan.FromSeconds(5));
            _producer?.Dispose();
            _disposed = true;
        }
    }
}