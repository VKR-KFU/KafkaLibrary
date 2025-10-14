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
        services
            .AddOptions<KafkaSettings>()
            .Bind(config.GetSection("Kafka"))
            .Validate(s => !string.IsNullOrWhiteSpace(s.BootstrapServers), "Kafka:BootstrapServers required")
            .Validate(s => !string.IsNullOrWhiteSpace(s.Topic),            "Kafka:Topic required")
            .Validate(s => !string.IsNullOrWhiteSpace(s.GroupId),          "Kafka:GroupId required")
            .ValidateOnStart();

        services.AddScoped(typeof(IKafkaProducerService<,>), typeof(KafkaProducerService<,>));
        services.AddScoped(typeof(IKafkaConsumerService<,>), typeof(KafkaConsumerService<,>));

        return services;
    }
}