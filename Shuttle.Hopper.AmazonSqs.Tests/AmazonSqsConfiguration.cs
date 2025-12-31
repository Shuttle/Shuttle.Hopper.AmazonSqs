using Amazon.Runtime;
using Amazon.SQS;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Shuttle.Hopper.AmazonSqs.Tests;

public static class AmazonSqsConfiguration
{
    public static IServiceCollection GetServiceCollection()
    {
        var services = new ServiceCollection();

        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddHopper(hopperBuilder =>
        {
            hopperBuilder.UseAmazonSqs(builder =>
            {
                var amazonSqsOptions = new AmazonSqsOptions
                {
                    WaitTime = TimeSpan.FromSeconds(1),
                    MaxMessages = 10,
                    AwsCredentials = new BasicAWSCredentials("test", "test"),
                    AmazonSqsConfig = new AmazonSQSConfig
                    {
                        ServiceURL = "http://localhost:9324", // ElasticMQ default
                        AuthenticationRegion = "us-east-1"
                    }
                };

                builder.AddOptions("local", amazonSqsOptions);
            });
        });

        return services;
    }
}