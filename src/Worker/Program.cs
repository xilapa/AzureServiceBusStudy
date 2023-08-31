using Worker;

var cousera = new ServiceBus();

await cousera.StartConsume();
await cousera.SendLoop();