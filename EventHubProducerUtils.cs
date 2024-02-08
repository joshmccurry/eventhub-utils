using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Eventhub_Utilities_Console {
	public class EventHubProducerUtils {
		EventHubProducerClient producer;
		public EventHubProducerUtils(string connectionstring, string evnamespace) {
				var connectionString = connectionstring;
				var eventHubName = evnamespace;
				producer = new EventHubProducerClient(connectionString, eventHubName);
		}

		public async Task SendMessageToEventHub(string message) {
				using EventDataBatch eventBatch = await producer.CreateBatchAsync();
				eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(message)));
				await producer.SendAsync(eventBatch);
		}
	}
}
