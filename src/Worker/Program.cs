using Worker;

var eventHub = new EventHub();
eventHub.Send();
await eventHub.Consume();