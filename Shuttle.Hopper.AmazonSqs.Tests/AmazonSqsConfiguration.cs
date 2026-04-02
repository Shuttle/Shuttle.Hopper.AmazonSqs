using Amazon.Runtime;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Shuttle.Hopper.AmazonSqs.Tests;

public static class AmazonSqsConfiguration
{
    public static IServiceCollection GetServiceCollection()
    {
        var services = new ServiceCollection();

        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddHopper()
            .UseAmazonSqs(builder =>
            {
                builder.Configure("local", options =>
                {
                    options.WaitTime = TimeSpan.FromSeconds(1);
                    options.MaxMessages = 10;
                    options.AwsCredentials = new BasicAWSCredentials("test", "test");
                    options.AmazonSqsConfig = new()
                    {
                        ServiceURL = "http://localhost:9324", // ElasticMQ default
                        AuthenticationRegion = "us-east-1"
                    };
                });
            });

        return services;
    }
}