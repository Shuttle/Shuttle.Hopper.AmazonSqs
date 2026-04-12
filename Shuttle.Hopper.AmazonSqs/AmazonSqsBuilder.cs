using Microsoft.Extensions.DependencyInjection;
using Shuttle.Contract;

namespace Shuttle.Hopper.AmazonSqs;

public class AmazonSqsBuilder(IServiceCollection services)
{
    public AmazonSqsBuilder Configure(string name, Action<AmazonSqsOptions> configureOptions)
    {
        Guard.AgainstNull(services)
            .AddOptions<AmazonSqsOptions>(Guard.AgainstEmpty(name))
            .Configure(Guard.AgainstNull(configureOptions));

        return this;
    }
}