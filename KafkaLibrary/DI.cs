using KafkaLibrary.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaLibrary;

public static class DI
{
    /// <summary>
    /// Конфигурация Kafka
    /// </summary>
    /// <param name="services">Ваши сервисы</param>
    /// <param name="config">[
    ///     Kafka: {
    ///         "BootstrapServers": "value"
    ///         "Topic": "value",
    ///         "GroupId": "value"
    ///     }
    /// 
    /// ]</param>
    /// <returns></returns>
    public static IServiceCollection AddKafka(this IServiceCollection services, IConfiguration config)
    {
        var kafkaSettings = new KafkaSettings();
        config.GetSection("Kafka").Bind(kafkaSettings);
        services.AddSingleton(kafkaSettings);

        services.AddScoped(typeof(IKafkaProducerService<,>), typeof(KafkaProducerService<,>));
        services.AddScoped(typeof(IKafkaConsumerService<,>), typeof(KafkaConsumerService<,>));

        return services;
    }
}