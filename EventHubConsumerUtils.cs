using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Eventhub_Utilities_Console {
	public class EventHubConsumerUtils {
		EventHubConsumerClient consumer;
		public EventHubConsumerUtils(string connectionstring, string evnamespace, string? consumerGroupName) {
			var connectionString = connectionstring;
			var eventHubName = evnamespace;
			var consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;
			if (consumerGroupName != null && consumerGroupName.Length > 0)
				consumerGroup = consumerGroupName;

			var consumerOptions = new EventHubConsumerClientOptions {
				RetryOptions = new EventHubsRetryOptions {
					Mode = EventHubsRetryMode.Exponential,
					MaximumRetries = 5,
					Delay = TimeSpan.FromMilliseconds(800),
					MaximumDelay = TimeSpan.FromSeconds(10)
				}
			};

			consumer = new EventHubConsumerClient(
				consumerGroup,
				connectionString,
				eventHubName,
				consumerOptions);
		}

		/// <summary>
		///   Reads events from all partitions of the event hub as an asynchronous enumerable, allowing events to be iterated as they
		///   become available on the partition, waiting as necessary should there be no events available.
		///
		///   This enumerator may block for an indeterminate amount of time for an <c>await</c> if events are not available on the partition, requiring
		///   cancellation via the <paramref name="cancellationToken"/> to be requested in order to return control.  It is recommended to set the
		///   <see cref="ReadEventOptions.MaximumWaitTime" /> for scenarios where a more deterministic maximum waiting period is desired.
		/// </summary>
		/// 
		/// <note>
		///   It is important to note that this method does not guarantee fairness amongst the partitions during iteration; each of the partitions compete to publish
		///   events to be read by the enumerator.  Depending on service communication, there may be a clustering of events per partition and/or there may be a noticeable
		///   bias for a given partition or subset of partitions.
		/// </note>
		public async Task<List<EventData>> ReadMessagesFromEventHub(int maxMessages) {
			List<EventData> messages = new();
			int count = 0;
			await foreach (PartitionEvent receivedEvent in consumer.ReadEventsAsync()) {
				if (count++ >= maxMessages)
					break;
				Console.WriteLine($"Message received on partition {receivedEvent.Partition.PartitionId}: {Encoding.UTF8.GetString(receivedEvent.Data.Body.ToArray())}");
				messages.Add(receivedEvent.Data);
			}
			return messages;
		}

		/// <summary>
		///   Retrieves the set of identifiers for the partitions of an Event Hub and serially processes each partition to read events from it.
		///   Order of partition ids are not guaranteed or sorted.
		/// </summary>
		public async Task<List<EventData>> ReadAllMessagesFromEventHub() {
			List<EventData> messages = new();
			foreach (var id in await consumer.GetPartitionIdsAsync()) {
				messages.AddRange(await ReadAllMessageFromPartitionEarliest(id));
			}
			return messages;
		}

		/// <summary>
		///   Corresponds to the location of the first event present in the partition.  Use this
		///   position to begin receiving from the first event that was enqueued in the partition
		///   which has not expired due to the retention policy.
		/// </summary>
		///
		public async Task<List<EventData>> ReadAllMessageFromPartitionEarliest(string partitionId) {
			return await ReadAllMessagesFromOffset(partitionId, EventPosition.Earliest);
		}

		/// <summary>
		///   Corresponds to a specific date and time within the partition to begin seeking an event; 
		///   the event enqueued on or after the specified <paramref name="time" /> will be read.
		/// </summary>
		public async Task<List<EventData>> ReadAllMessageFromPartitionDateOffset(string partitionId, DateTime time) {
			return await ReadAllMessagesFromOffset(partitionId, EventPosition.FromEnqueuedTime(time));
		}

		/// <summary>
		///   Corresponds to a specific EventPosition within the partition to begin seeking an event; 
		///   the event enqueued will be compared with the specified position <paramref name="psition" /> will be read.
		/// </summary>
		public async Task<List<EventData>> ReadAllMessagesFromOffset(string partitionId, EventPosition position) {
			List<EventData> messages = new();
			await foreach (PartitionEvent receivedEvent in consumer.ReadEventsFromPartitionAsync(partitionId, position)) {
				Console.WriteLine($"Message received on partition {receivedEvent.Partition.PartitionId}: {Encoding.UTF8.GetString(receivedEvent.Data.Body.ToArray())}");
				messages.Add(receivedEvent.Data);
			}
			return messages;
		}

	}
}
