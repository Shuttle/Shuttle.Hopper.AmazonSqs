using NUnit.Framework;
using Shuttle.Hopper.Testing;

namespace Shuttle.Hopper.AmazonSqs.Tests;

public class AmazonSqsQueueOutboxFixture : OutboxFixture
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_handle_errors_async(bool isTransactionalEndpoint)
    {
        await TestOutboxSendingAsync(AmazonSqsConfiguration.GetServiceCollection(), "amazonsqs://local/{0}", 3, isTransactionalEndpoint);
    }
}