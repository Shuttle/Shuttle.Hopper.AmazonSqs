using NUnit.Framework;
using Shuttle.Hopper.Testing;

namespace Shuttle.Hopper.AmazonSqs.Tests;

public class AmazonSqsQueueInboxFixture : InboxFixture
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_handle_errors_async(bool hasErrorQueue)
    {
        await TestInboxErrorAsync(AmazonSqsConfiguration.GetServiceCollection(), "amazonsqs://local/{0}", hasErrorQueue);
    }

    [Test]
    public async Task Should_be_able_to_handle_a_deferred_message_async()
    {
        await TestInboxDeferredAsync(AmazonSqsConfiguration.GetServiceCollection(), "amazonsqs://local/{0}");
    }

    [Test]
    public async Task Should_be_able_to_process_messages_concurrently_async()
    {
        await TestInboxConcurrencyAsync(AmazonSqsConfiguration.GetServiceCollection(), "amazonsqs://local/{0}", TimeSpan.FromSeconds(5));
    }

    [Test]
    public async Task Should_be_able_to_process_queue_timeously_async()
    {
        await TestInboxThroughputAsync(AmazonSqsConfiguration.GetServiceCollection(), "amazonsqs://local/{0}", 1000, 5);
    }
}