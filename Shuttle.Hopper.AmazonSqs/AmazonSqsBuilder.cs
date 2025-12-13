using Microsoft.Extensions.DependencyInjection;
using Shuttle.Core.Contract;

namespace Shuttle.Hopper.AmazonSqs;

public class AmazonSqsBuilder(IServiceCollection services)
{
    internal readonly Dictionary<string, AmazonSqsOptions> AmazonSqsOptions = new();

    public IServiceCollection Services { get; } = Guard.AgainstNull(services);

    public AmazonSqsBuilder AddOptions(string name, AmazonSqsOptions amazonSqsOptions)
    {
        Guard.AgainstEmpty(name);
        Guard.AgainstNull(amazonSqsOptions);

        AmazonSqsOptions.Remove(name);

        AmazonSqsOptions.Add(name, amazonSqsOptions);

        return this;
    }
}