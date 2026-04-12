using Microsoft.Extensions.DependencyInjection;

namespace Shuttle.Hopper.AmazonSqs;

public static class HopperBuilderExtensions
{
    extension(HopperBuilder hopperBuilder)
    {
        public HopperBuilder UseAmazonSqs(Action<AmazonSqsBuilder>? builder = null)
        {
            var services = hopperBuilder.Services;

            builder?.Invoke(new(services));

            services.PostConfigureAll<AmazonSqsOptions>(options =>
            {
                options.MaxMessages = Math.Clamp(options.MaxMessages, 1, 10);

                if (options.WaitTime < TimeSpan.Zero)
                {
                    options.WaitTime = TimeSpan.Zero;
                }

                if (options.WaitTime > TimeSpan.FromSeconds(20))
                {
                    options.WaitTime = TimeSpan.FromSeconds(20);
                }
            });

            services.AddSingleton<ITransportFactory, AmazonSqsQueueFactory>();

            return hopperBuilder;
        }
    }
}