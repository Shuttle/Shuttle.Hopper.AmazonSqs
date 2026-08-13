using Amazon.Runtime;
using Amazon.SQS;

namespace Shuttle.Hopper.AmazonSqs;

public class AmazonSqsOptions
{
    public const string SectionName = "Shuttle:AmazonSqs";
    public AWSCredentials? AwsCredentials { get; set; }
    public AmazonSQSConfig? AmazonSqsConfig { get; set; }
    public int MaxMessages { get; set; } = 10;
    public TimeSpan WaitTime { get; set; } = TimeSpan.FromSeconds(20);
    public TimeSpan? VisibilityTimeout { get; set; }
}