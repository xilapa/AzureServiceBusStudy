using Azure.Core;
using Azure.Storage.Queues;

namespace Worker;

public sealed class QueueStorage
{
    private readonly QueueClient _queueClient;

    public QueueStorage()
    {
        var connectionString = "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=xilapastoragelearn;AccountKey=s9NrLnpob8ggm5wOfZR2vZ4THwE8S/vJtRt94eOGTyLobJidlBia7GstHaSUheuIM4zMMm9R/zPu+AStnfN8gw==;BlobEndpoint=https://xilapastoragelearn.blob.core.windows.net/;FileEndpoint=https://xilapastoragelearn.file.core.windows.net/;QueueEndpoint=https://xilapastoragelearn.queue.core.windows.net/;TableEndpoint=https://xilapastoragelearn.table.core.windows.net/";
        _queueClient = new QueueClient(connectionString, "my-queue", new QueueClientOptions
        {
            Retry = { MaxRetries = 5, Mode = RetryMode.Exponential},
            MessageEncoding = QueueMessageEncoding.Base64
        });
    }

    public async Task Start()
    {
        await _queueClient.CreateIfNotExistsAsync();

        var exit = false;
        while (!exit)
        {
            Console.WriteLine("What operation would you like to perform?");
            Console.WriteLine("  1 - Send message");
            Console.WriteLine("  2 - Peek at the next message");
            Console.WriteLine("  3 - Receive message");
            Console.WriteLine("  X - Exit program");
            
            var option = Console.ReadKey();
            Console.WriteLine();

            switch (option.KeyChar)
            {
                case '1':
                    Console.WriteLine("Enter the message:");
                    var msgToSend = Console.ReadLine();
                    Console.WriteLine();
                    var receipt = await _queueClient.SendMessageAsync(msgToSend);
                    Console.WriteLine($"Message sent, Id: {receipt.Value.MessageId}");
                    break;
                case '2':
                    var nextMsg = await _queueClient.PeekMessageAsync();
                    Console.WriteLine(nextMsg?.Value?.Body);
                    Console.WriteLine();
                    break;
                case '3':
                    var msg = await _queueClient.ReceiveMessageAsync(TimeSpan.FromMinutes(5));
                    Console.WriteLine(msg.Value.Body);
                    await _queueClient.DeleteMessageAsync(msg.Value.MessageId, msg.Value.PopReceipt);
                    Console.WriteLine();
                    break;
                default:
                    exit = true;
                    break;
            }
        }
    }
}