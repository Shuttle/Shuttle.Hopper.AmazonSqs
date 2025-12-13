using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;

namespace Shuttle.Hopper.AmazonSqs;

public static class ServiceCollectionExtensions
{
    extension(IServiceCollection services)
    {
        public IServiceCollection AddAmazonSqs(Action<AmazonSqsBuilder>? builder = null)
        {
            Guard.AgainstNull(services);

            var amazonSqsBuilder = new AmazonSqsBuilder(services);

            builder?.Invoke(amazonSqsBuilder);

            foreach (var pair in amazonSqsBuilder.AmazonSqsOptions)
            {
                services.AddOptions<AmazonSqsOptions>(pair.Key).Configure(options =>
                {
                    options.AwsCredentials = pair.Value.AwsCredentials;
                    options.AmazonSqsConfig = pair.Value.AmazonSqsConfig;
                    options.MaxMessages = pair.Value.MaxMessages;
                    options.WaitTime = pair.Value.WaitTime;

                    if (options.MaxMessages < 1)
                    {
                        options.MaxMessages = 1;
                    }

                    if (options.MaxMessages > 10)
                    {
                        options.MaxMessages = 10;
                    }

                    if (options.WaitTime < TimeSpan.Zero)
                    {
                        options.WaitTime = TimeSpan.Zero;
                    }

                    if (options.WaitTime > TimeSpan.FromSeconds(20))
                    {
                        options.WaitTime = TimeSpan.FromSeconds(20);
                    }
                });
            }

            services.TryAddSingleton<ITransportFactory, AmazonSqsQueueFactory>();

            return services;
        }
    }
}