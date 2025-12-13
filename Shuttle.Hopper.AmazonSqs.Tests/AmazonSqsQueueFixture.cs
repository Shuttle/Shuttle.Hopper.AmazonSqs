using NUnit.Framework;
using Shuttle.Hopper.Testing;

namespace Shuttle.Hopper.AmazonSqs.Tests;

[TestFixture]
public class AmazonSqsQueueFixture : BasicTransportFixture
{
    [Test]
    public async Task Should_be_able_to_perform_simple_enqueue_and_get_message_async()
    {
        await TestSimpleSendAndReceiveAsync(AmazonSqsConfiguration.GetServiceCollection(), "amazonsqs://local/{0}");
        await TestSimpleSendAndReceiveAsync(AmazonSqsConfiguration.GetServiceCollection(), "amazonsqs://local/{0}-transient");
    }

    [Test]
    public async Task Should_be_able_to_release_a_message_async()
    {
        await TestReleaseMessageAsync(AmazonSqsConfiguration.GetServiceCollection(), "amazonsqs://local/{0}");
    }

    [Test]
    public async Task Should_be_able_to_get_message_again_when_not_acknowledged_before_queue_is_disposed_async()
    {
        await TestUnacknowledgedMessageAsync(AmazonSqsConfiguration.GetServiceCollection(), "amazonsqs://local/{0}");
    }
}