using Azure.Messaging.EventHubs;
using Eventhub_Utilities_Console;
using System.Text.Json;

Console.Write("Please enter the connection string: ");
var connectionstring = Console.ReadLine();
Console.Write("Please enter the namespace: ");
var evnamespace = Console.ReadLine();
Console.Write("Please enter the consumer Group (null for default): ");
var consumegroup = Console.ReadLine();
EventHubConsumerUtils eventHubUtils = new(connectionstring, evnamespace, consumegroup);

var messages = await eventHubUtils.ReadAllMessagesFromEventHub();
Console.WriteLine($"Received {messages.Count} messages from the event hub");

Console.Write("Reload Message (Y/n): ");
var reload = Console.ReadLine();
if(reload.ToLower() == "y") {
	EventHubProducerUtils eventHubProducerUtils = new(connectionstring, evnamespace);
	foreach (var message in messages)
		await eventHubProducerUtils.SendMessageToEventHub(JsonSerializer.Serialize<EventData>(message));
	Console.WriteLine("Message sent to the event hub");
}else {
	Console.WriteLine("Exiting...\nPress Any Key...");
	Console.ReadLine();
}
