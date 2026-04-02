using Microsoft.Extensions.DependencyInjection;

namespace Shuttle.Hopper.AmazonSqs;

public static class HopperBuilderExtensions
{
    extension(HopperBuilder hopperBuilder)
    {
        public HopperBuilder UseAmazonSqs(Action<AmazonSqsBuilder>? builder = null)
        {
            var services = hopperBuilder.Services;

            var amazonSqsBuilder = new AmazonSqsBuilder();

            builder?.Invoke(amazonSqsBuilder);

            foreach (var pair in amazonSqsBuilder.AmazonSqsConfigureOptions)
            {
                services.AddOptions<AmazonSqsOptions>(pair.Key).Configure(options =>
                {
                    pair.Value(options);

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

            services.AddSingleton<ITransportFactory, AmazonSqsQueueFactory>();

            return hopperBuilder;
        }
    }
}