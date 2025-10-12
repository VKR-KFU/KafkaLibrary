using Confluent.Kafka;
using KafkaLibrary.JsonSerializers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaLibrary.Services;

public class KafkaConsumerService<TKey, TValue> : IKafkaConsumerService<TKey, TValue>
{
    private readonly IConsumer<TKey, TValue> _consumer;
    private readonly KafkaSettings _kafkaSettings;
    private readonly ILogger<KafkaConsumerService<TKey, TValue>> _logger;
    private bool _disposed = false;
    private string? _currentTopic = null;

    public KafkaConsumerService(
        IOptions<KafkaSettings> kafkaSettings,
        ILogger<KafkaConsumerService<TKey, TValue>> logger)
    {
        _kafkaSettings = kafkaSettings.Value;
        _logger = logger;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _kafkaSettings.BootstrapServers,
            GroupId = _kafkaSettings.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        };

        _consumer = new ConsumerBuilder<TKey, TValue>(consumerConfig)
            .SetValueDeserializer(new JsonDeserializer<TValue>())
            .SetErrorHandler((_, error) => { _logger.LogError("Kafka Consumer Error: {Reason}", error.Reason); })
            .Build();

        if (!string.IsNullOrEmpty(_kafkaSettings.Topic))
        {
            Subscribe(_kafkaSettings.Topic);
        }
    }

    public void Subscribe(string topic)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(KafkaConsumerService<TKey, TValue>));

        _consumer.Subscribe(topic);
        _currentTopic = topic;
        _logger.LogInformation("Subscribed to topic: {Topic}", topic);
    }

    public void Subscribe(IEnumerable<string> topics)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(KafkaConsumerService<TKey, TValue>));

        var topicsList = topics.ToList();
        _consumer.Subscribe(topicsList);
        _currentTopic = string.Join(", ", topicsList);
        _logger.LogInformation("Subscribed to topics: {Topics}", _currentTopic);
    }

    public void Unsubscribe()
    {
        if (_disposed)
            return;

        _consumer.Unsubscribe();
        _currentTopic = null;
        _logger.LogInformation("Unsubscribed from all topics");
    }

    public async Task<TValue?> ConsumeAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(KafkaConsumerService<TKey, TValue>));

        if (_currentTopic == null)
        {
            throw new InvalidOperationException("Consumer is not subscribed to any topic. Call Subscribe() first.");
        }

        return await ConsumeInternalAsync(cancellationToken);
    }

    public async Task<TValue?> ConsumeAsync(string topic, CancellationToken cancellationToken = default)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(KafkaConsumerService<TKey, TValue>));

        if (_currentTopic != topic)
        {
            Subscribe(topic);
        }

        return await ConsumeInternalAsync(cancellationToken);
    }

    private async Task<TValue?> ConsumeInternalAsync(CancellationToken cancellationToken)
    {
        try
        {
            var consumeResult = _consumer.Consume(cancellationToken);

            if (consumeResult.Message.Value != null)
            {
                _logger.LogDebug("Message consumed from {Topic}, partition {Partition}, offset {Offset}",
                    consumeResult.Topic, consumeResult.Partition, consumeResult.Offset);

                return consumeResult.Message.Value;
            }

            return default;
        }
        catch (ConsumeException ex)
        {
            _logger.LogError(ex, "Error consuming message from Kafka");
            throw;
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("Kafka consumption was cancelled");
            throw;
        }
    }

    public void Commit()
    {
        if (_disposed) 
            throw new ObjectDisposedException(nameof(KafkaConsumerService<TKey, TValue>));

        try
        {
            _consumer.Commit();
            _logger.LogDebug("Committed Kafka offsets");
        }
        catch (KafkaException ex)
        {
            _logger.LogError(ex, "Error committing Kafka offsets");
            throw;
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _consumer?.Close();
            _consumer?.Dispose();
            _disposed = true;
        }
    }
}