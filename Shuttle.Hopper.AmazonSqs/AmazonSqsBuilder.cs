using Shuttle.Contract;

namespace Shuttle.Hopper.AmazonSqs;

public class AmazonSqsBuilder
{
    internal readonly Dictionary<string, Action<AmazonSqsOptions>> AmazonSqsConfigureOptions = new();

    public AmazonSqsBuilder Configure(string name, Action<AmazonSqsOptions> configure)
    {
        Guard.AgainstEmpty(name);
        Guard.AgainstNull(configure);

        AmazonSqsConfigureOptions.Remove(name);
        AmazonSqsConfigureOptions.Add(name, configure);

        return this;
    }
}