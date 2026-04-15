# Amazon SQS

Amazon Simple Queue Service (SQS) implementation for use with Shuttle.Hopper, providing reliable message queuing capabilities using AWS SQS.

In order to make use of the `AmazonSqsQueue` you will need access to an [Amazon Web Services](https://aws.amazon.com/sqs/) account. There are some options for local development, such as [ElasticMQ](https://github.com/softwaremill/elasticmq), which are beyond the scope of this documentation.

You may also want to take a look at [Messaging Using Amazon SQS](https://docs.aws.amazon.com/sdk-for-net/v3/developer-guide/sqs-apis-intro.html).

## Installation

```bash
dotnet add package Shuttle.Hopper.AmazonSqs
```

## Configuration

The URI structure is `amazonsqs://configuration-name/queue-name`.

### Programmatic Configuration

```c#
services.AddHopper()
    .UseAmazonSqs(builder =>
    {
        builder.Configure("local", options =>
        {
            options.AwsCredentials = new BasicAWSCredentials("accessKey", "secretKey");
            options.AmazonSqsConfig = new()
            {
                ServiceURL = "http://localhost:9324",
                AuthenticationRegion = "us-east-1"
            };
            options.MaxMessages = 5;
            options.WaitTime = TimeSpan.FromSeconds(20);
        });
    });
```

### JSON Configuration

The default JSON settings structure is as follows:

```json
{
  "Shuttle": {
    "AmazonSqs": {
      "local": {
        "MaxMessages": 5,
        "WaitTime": "00:00:20",
        "AmazonSqsConfig": {
          "ServiceURL": "http://localhost:9324"
        }
      },
      "production": {
        "MaxMessages": 10,
        "WaitTime": "00:00:20",
        "AmazonSqsConfig": {
          "ServiceURL": "https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue"
        }
      }
    }
  }
}
```

> [!IMPORTANT]
> Complex types like `AwsCredentials` and detailed `AmazonSqsConfig` properties must be configured programmatically. JSON configuration supports only simple properties like `MaxMessages` and `WaitTime`, plus the `ServiceURL` within `AmazonSqsConfig`.

### URI Format Examples

```
amazonsqs://local/my-queue
amazonsqs://production/order-processing-queue
```

The configuration name (e.g., `local`, `production`) must match a configuration defined via `builder.Configure("name", ...)`.

## `AmazonSqsOptions`

| Option | Default | Description |
| --- | --- | --- |
| `AwsCredentials` | `null` | AWS credentials for authentication. If `null`, the AWS SDK will use the default credential provider chain (environment variables, IAM roles, etc.). |
| `AmazonSqsConfig` | `null` | AWS SQS configuration object. If `null`, a default configuration will be created. Use this to set `ServiceURL`, `AuthenticationRegion`, and other AWS-specific settings. |
| `MaxMessages` | `10` | Specifies the number of messages to fetch from the queue in a single request. Valid range: 1-10 (values outside this range will be automatically clamped). |
| `WaitTime` | `00:00:20` | Specifies the `TimeSpan` duration to perform long-polling. Valid range: 00:00:00 to 00:00:20 (values outside this range will be automatically clamped). |

> [!NOTE]
> The `MaxMessages` value is automatically clamped between 1 and 10, as per AWS SQS API limits. The `WaitTime` is clamped between 0 and 20 seconds.

## Authentication

AWS credentials can be configured in several ways:

### Programmatic Credentials

```c#
options.AwsCredentials = new BasicAWSCredentials("accessKey", "secretKey");
```

### Environment Variables

If `AwsCredentials` is not set, the AWS SDK will automatically use credentials from:
- `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables
- AWS credentials file (`~/.aws/credentials`)
- IAM role (when running on EC2 or ECS)

### IAM Roles (Recommended for Production)

When running on AWS infrastructure, use IAM roles instead of hardcoded credentials.

## Troubleshooting

### Queue URL Not Resolved

If you see errors about unresolved queue URLs, ensure:
1. The queue exists in your AWS account
2. The configuration name in the URI matches your configured options
3. Your AWS credentials have permission to access the queue

### Authentication Errors

If you encounter authentication errors:
1. Verify your AWS credentials are correct
2. Check that the `AuthenticationRegion` matches your queue's region
3. Ensure your IAM user/role has the necessary SQS permissions

### Configuration Name Mismatch

The configuration name in the URI (e.g., `amazonsqs://local/queue-name`) must exactly match a configuration added via `builder.Configure("local", ...)`. Configuration names are case-sensitive.
