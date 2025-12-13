using Microsoft.Extensions.Configuration;
using NUnit.Framework;

namespace Shuttle.Hopper.AmazonSqs.Tests;

[TestFixture]
public class AmazonSqsOptionsFixture
{
    protected AmazonSqsOptions GetSettings(string name)
    {
        var result = new AmazonSqsOptions();

        new ConfigurationBuilder()
            .AddJsonFile(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @".\appsettings.json")).Build()
            .GetRequiredSection($"{AmazonSqsOptions.SectionName}:{name}").Bind(result);

        return result;
    }

    [Test]
    public void Should_be_able_to_load_a_full_configuration()
    {
        var endpointA = GetSettings("endpoint-a");

        Assert.That(endpointA.AmazonSqsConfig?.ServiceURL.Contains("us-east-1"), Is.True);
        Assert.That(endpointA.AmazonSqsConfig?.ServiceURL.Contains("/MyQueue"), Is.True);

        var endpointB = GetSettings("endpoint-b");

        Assert.That(endpointB.AmazonSqsConfig?.ServiceURL.Contains("us-east-2"), Is.True);
        Assert.That(endpointB.AmazonSqsConfig?.ServiceURL.Contains("/MyQueue"), Is.True);
    }
}