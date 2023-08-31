using Azure.Messaging.ServiceBus;

namespace Worker;

public class ServiceBus
{
    private readonly ServiceBusSender _queueSender;
    private readonly ServiceBusSender _topicSender;
    private readonly ServiceBusProcessor _queueProcessor;
    private readonly ServiceBusProcessor _topicProcessorAmericas;
    private readonly ServiceBusProcessor _topicProcessorEuropeAndAsia;
    private readonly ServiceBusProcessor[] _serviceBusEntities;

    public ServiceBus()
    {
        var connectionString = "Endpoint=sb://xilapa-mslearn.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=rJKgp25VvK0vWo3fqsd0xdeUltYvx3+DY+ASbAhDz3Q=";
        var queueName = "salesmessages";
        var topicName = "salesperformancemessages";

        var serviceBusClient = new ServiceBusClient(connectionString);

        _queueSender = serviceBusClient.CreateSender(queueName);
        _topicSender = serviceBusClient.CreateSender(topicName);

        var processorOptions = new ServiceBusProcessorOptions
        {
            PrefetchCount = 2,
            AutoCompleteMessages = false,
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            MaxConcurrentCalls = 2,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(5)
        };

        _queueProcessor = serviceBusClient.CreateProcessor(queueName, processorOptions);
        _topicProcessorAmericas = serviceBusClient.CreateProcessor(topicName, "Americas", processorOptions);
        _topicProcessorEuropeAndAsia = serviceBusClient.CreateProcessor(topicName, "EuropeAndAsia", processorOptions);

        _serviceBusEntities = new [] { _queueProcessor, _topicProcessorAmericas, _topicProcessorEuropeAndAsia };

        foreach (var entity in _serviceBusEntities)
        {
            entity.ProcessMessageAsync += ConsumeMessages;
            entity.ProcessErrorAsync += ConsumeErrors;
        }
    }

    public async Task SendLoop()
    {
        ConsoleKeyInfo input;
        do
        {
            Console.WriteLine("Select:\n 1 to send a message to the queue\n 2 to send a message to the topic\n ESC to exit\n");
            input = Console.ReadKey();
            if (input.Key == ConsoleKey.Escape) break;
            Console.WriteLine("\ntype a message:\n");
            var msg = new ServiceBusMessage(Console.ReadLine() ?? string.Empty);

            switch (input.KeyChar)
            {
                case '1':
                    await _queueSender.SendMessageAsync(msg);
                    break;
                case '2':
                    await _topicSender.SendMessageAsync(msg);
                    break;
            }

        } while (input.Key != ConsoleKey.Escape);
    }

    public async Task StartConsume()
    {
        foreach (var entity in _serviceBusEntities)
            await entity.StartProcessingAsync();
    }

    private async Task ConsumeMessages(ProcessMessageEventArgs messageArgs)
    {
        var rand = Random.Shared.Next();
        if (rand % 2 == 0) 
            throw new Exception("random error");

        Console.WriteLine(messageArgs.EntityPath + " - Message received: " + messageArgs.Message.Body + "\n");
        await messageArgs.CompleteMessageAsync(messageArgs.Message, CancellationToken.None);
    }
    
    private async Task ConsumeErrors(ProcessErrorEventArgs messageArgs)
    {
        Console.WriteLine(messageArgs.EntityPath  + " - Error received: " + messageArgs.Exception.Message);
    }
}