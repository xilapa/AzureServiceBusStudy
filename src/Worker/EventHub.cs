using System.Text;
using System.Text.Json;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;

namespace Worker;

public class EventHub
{
    private readonly string _connectionString;
    private readonly string _eventHubName;

    public EventHub()
    {
        _connectionString = "Endpoint=sb://xilapa-hub-learn.servicebus.windows.net/;" +
                               "SharedAccessKeyName=RootManageSharedAccessKey;" +
                               "SharedAccessKey=oT2imbF3VTJt12wLkODU4PwSbgRwb/hdO+AEhPWwY3c=";
        _eventHubName = "tsthub";
    }


    public async Task Send()
    {
        var producer = new EventHubProducerClient(_connectionString, _eventHubName);
        using var eventBatch = await producer.CreateBatchAsync();

        foreach (var i in Enumerable.Range(0, 1000))
            eventBatch.TryAdd(new EventData($"{DateTime.Now}:event:{i}"));

        await producer.SendAsync(eventBatch);

        await producer.DisposeAsync();
    }

    public async Task Consume()
    {
        var storageAccountConnectionString = "DefaultEndpointsProtocol=https;" +
             "AccountName=cloudshell209093851;" +
             "AccountKey=L0AxMoyq31o3dGLX5GS/8fpmgBswZ/D47v1M4ZjRToZUQrfXJT3/ASsxEQkyBjVOAAvWabDjQobi+AStAwdrSw==;" +
             "EndpointSuffix=core.windows.net";
        var storageContainer = new BlobContainerClient(storageAccountConnectionString, "messages");

        var processor = new EventProcessorClient(storageContainer, "local-worker", _connectionString, _eventHubName);

        processor.ProcessEventAsync += ProcessEvent;
        processor.ProcessErrorAsync += ProcessError;

        await processor.StartProcessingAsync();

        await Task.Delay(TimeSpan.FromSeconds(600));

        await processor.StopProcessingAsync();
    }


    private Task ProcessEvent(ProcessEventArgs eventArgs)
    {
        if (Random.Shared.Next() % 2 == 0)
            throw new Exception("Random error");

        Console.WriteLine($"{DateTime.Now}:EventId:{eventArgs.Data.MessageId}:Processed Event:{eventArgs.Data.Data}");
        return Task.CompletedTask;
    }
    
    private Task ProcessError(ProcessErrorEventArgs eventArgs)
    {
        Console.WriteLine($"{DateTime.Now}:Processed Error:{eventArgs.Exception.Message}" +
                          $"Inner Exception:\n\n{eventArgs.Exception.InnerException?.Message}");
        return Task.CompletedTask;
    }
}