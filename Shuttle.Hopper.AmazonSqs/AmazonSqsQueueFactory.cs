using Amazon.SQS;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;

namespace Shuttle.Hopper.AmazonSqs;

public class AmazonSqsQueueFactory(IOptions<HopperOptions> hopperOptions, IOptionsMonitor<AmazonSqsOptions> amazonSqsOptions) : ITransportFactory
{
    private readonly HopperOptions _hopperOptions = Guard.AgainstNull(Guard.AgainstNull(hopperOptions).Value);
    private readonly IOptionsMonitor<AmazonSqsOptions> _amazonSqsOptions = Guard.AgainstNull(amazonSqsOptions);

    public string Scheme => "amazonsqs";

    public Task<ITransport> CreateAsync(Uri uri, CancellationToken cancellationToken =default)
    {
        var transportUri = new TransportUri(Guard.AgainstNull(uri)).SchemeInvariant(Scheme);
        var amazonSqsOptions = _amazonSqsOptions.Get(transportUri.ConfigurationName);

        if (amazonSqsOptions == null)
        {
            throw new InvalidOperationException(string.Format(Hopper.Resources.TransportConfigurationNameException, transportUri.ConfigurationName));
        }

        return Task.FromResult<ITransport>(new AmazonSqsQueue(_hopperOptions, amazonSqsOptions, transportUri));
    }
}