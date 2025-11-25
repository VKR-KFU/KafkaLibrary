namespace KafkaLibrary.Topics;

public static class TopicConstants
{
    /// <summary>
    /// Топик для отправки в Elibrary
    /// </summary>
    public static string ELibraryTopic = "elibrary-topic";

    /// <summary>
    /// Топик для обновленной статьи
    /// </summary>
    public static string ArticleUpdateEventTopic = "article-updated";
    
    /// <summary>
    /// Топик для сохранения данных в postgres
    /// </summary>
    public static string DataSaverTopic = "data-saver-topic";
}