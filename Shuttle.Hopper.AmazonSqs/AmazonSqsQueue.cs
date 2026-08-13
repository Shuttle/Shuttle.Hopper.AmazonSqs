using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Shuttle.Contract;
using Shuttle.Streams;
using Shuttle.Pipelines;

namespace Shuttle.Hopper.AmazonSqs;

public class AmazonSqsQueue : ITransport, ICreateTransport, IDeleteTransport, IPurgeTransport, IDisposable
{
    private readonly ILogger<AmazonSqsQueue> _logger;
    private readonly Dictionary<string, AcknowledgementToken> _acknowledgementTokens = new();
    private readonly AmazonSqsOptions _amazonSqsOptions;

    private readonly AmazonSQSClient _client;

    private readonly List<string> _isEmptyAttributeNames =
    [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesDelayed",
        "ApproximateNumberOfMessagesNotVisible"
    ];

    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly TimeSpan _operationTimeout = TimeSpan.FromSeconds(30);
    private readonly Queue<ReceivedMessage> _receivedMessages = new();
    private readonly HopperOptions _hopperOptions;
    private bool _initialized;
    private string _queueUrl = string.Empty;
    private bool _queueUrlResolved;

    // matches the Amazon SQS service default applied when no visibility timeout is specified
    private readonly TimeSpan _visibilityTimeout;
    private readonly Timer _visibilityTimeoutRenewalTimer;

    public AmazonSqsQueue(HopperOptions hopperOptions, AmazonSqsOptions amazonSqsOptions, TransportUri uri, ILogger<AmazonSqsQueue>? logger = null)
    {
        _logger = logger ?? NullLogger<AmazonSqsQueue>.Instance;
        _hopperOptions = Guard.AgainstNull(hopperOptions);
        _amazonSqsOptions = Guard.AgainstNull(amazonSqsOptions);

        Uri = Guard.AgainstNull(uri);

        _client = amazonSqsOptions.AwsCredentials == null
            ? new(amazonSqsOptions.AmazonSqsConfig ?? new AmazonSQSConfig())
            : new(amazonSqsOptions.AwsCredentials, amazonSqsOptions.AmazonSqsConfig ?? new AmazonSQSConfig());

        _visibilityTimeout = _amazonSqsOptions.VisibilityTimeout ?? TimeSpan.FromSeconds(30);

        var renewalInterval = TimeSpan.FromTicks(_visibilityTimeout.Ticks / 2);

        if (renewalInterval < TimeSpan.FromSeconds(1))
        {
            renewalInterval = TimeSpan.FromSeconds(1);
        }

        _visibilityTimeoutRenewalTimer = new(OnVisibilityTimeoutRenewalTimer, null, renewalInterval, renewalInterval);
    }

    public async Task CreateAsync(CancellationToken cancellationToken = default)
    {
        LogMessage.Operation(_logger, Uri.Uri.Scheme, Uri.TransportName, "[create/starting]");

        await _hopperOptions.TransportOperation.InvokeAsync(new(this, "[create/starting]"), cancellationToken);

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            await _client.CreateQueueAsync(new CreateQueueRequest { QueueName = Uri.TransportName }, cancellationToken).ConfigureAwait(false);
            await GetQueueUrl(cancellationToken);
        }
        finally
        {
            _lock.Release();
        }

        LogMessage.Operation(_logger, Uri.Uri.Scheme, Uri.TransportName, "[create/completed]");

        await _hopperOptions.TransportOperation.InvokeAsync(new(this, "[create/completed]"), cancellationToken);
    }

    public async Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        if (!_initialized)
        {
            await GetQueueUrl(cancellationToken);
        }

        if (!_queueUrlResolved)
        {
            return;
        }

        LogMessage.Operation(_logger, Uri.Uri.Scheme, Uri.TransportName, "[drop/starting]");

        await _hopperOptions.TransportOperation.InvokeAsync(new(this, "[drop/starting]"), cancellationToken);

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            await _client.DeleteQueueAsync(new DeleteQueueRequest { QueueUrl = _queueUrl }, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _lock.Release();
        }

        LogMessage.Operation(_logger, Uri.Uri.Scheme, Uri.TransportName, "[drop/completed]");

        await _hopperOptions.TransportOperation.InvokeAsync(new(this, "[drop/completed]"), cancellationToken);
    }

    public void Dispose()
    {
        _visibilityTimeoutRenewalTimer.Dispose();

        if (!_queueUrlResolved)
        {
            return;
        }

        _lock.Wait(CancellationToken.None);

        try
        {
            foreach (var acknowledgementToken in _acknowledgementTokens.Values)
            {
                _client.SendMessageAsync(new() { QueueUrl = _queueUrl, MessageBody = acknowledgementToken.MessageBody }).Wait(_operationTimeout);
                _client.DeleteMessageAsync(_queueUrl, acknowledgementToken.ReceiptHandle).Wait(_operationTimeout);
            }

            _acknowledgementTokens.Clear();
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task PurgeAsync(CancellationToken cancellationToken = default)
    {
        if (!_initialized)
        {
            await GetQueueUrl(cancellationToken);
        }

        if (!_queueUrlResolved)
        {
            return;
        }

        LogMessage.Operation(_logger, Uri.Uri.Scheme, Uri.TransportName, "[purge/starting]");

        await _hopperOptions.TransportOperation.InvokeAsync(new(this, "[purge/starting]"), cancellationToken);

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            await _client.PurgeQueueAsync(new PurgeQueueRequest { QueueUrl = _queueUrl }, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _lock.Release();
        }

        LogMessage.Operation(_logger, Uri.Uri.Scheme, Uri.TransportName, "[purge/completed]");

        await _hopperOptions.TransportOperation.InvokeAsync(new(this, "[purge/completed]"), cancellationToken);
    }

    public TransportUri Uri { get; }

    public async Task AcknowledgeAsync(object acknowledgementToken, IPipeline pipeline, CancellationToken cancellationToken = default)
    {
        Guard.AgainstNull(acknowledgementToken);

        if (!_initialized)
        {
            await GetQueueUrl(cancellationToken);
        }

        GuardAgainstUnresolvedQueueUrl();

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        if (!_queueUrlResolved)
        {
            await GetQueueUrl(cancellationToken);
        }

        if (acknowledgementToken is not AcknowledgementToken data)
        {
            return;
        }

        _acknowledgementTokens.Remove(data.MessageId);

        try
        {
            await _client.DeleteMessageAsync(_queueUrl, data.ReceiptHandle, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _lock.Release();
        }

        LogMessage.MessageAcknowledged(_logger, Uri.Uri.Scheme, Uri.TransportName);

        await _hopperOptions.MessageAcknowledged.InvokeAsync(new(this, acknowledgementToken, pipeline), cancellationToken);
    }

    public async Task SendAsync(Stream stream, IPipeline pipeline, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(pipeline);

        var transportMessage = Guard.AgainstNull(pipeline.State.GetTransportMessage());

        if (!_initialized)
        {
            await GetQueueUrl(cancellationToken);
        }

        GuardAgainstUnresolvedQueueUrl();

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            await _client.SendMessageAsync(new() { QueueUrl = _queueUrl, MessageBody = Convert.ToBase64String(await stream.ToBytesAsync()) }, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _lock.Release();
        }

        LogMessage.MessageEnqueued(_logger, Uri.Uri.Scheme, Uri.TransportName, transportMessage.MessageType, transportMessage.MessageId);

        await _hopperOptions.MessageSent.InvokeAsync(new(this, stream, pipeline), cancellationToken);
    }

    public TransportType Type => TransportType.Queue;

    public async Task<ReceivedMessage?> ReceiveAsync(IPipeline pipeline, CancellationToken cancellationToken = default)
    {
        if (!_initialized)
        {
            await GetQueueUrl(cancellationToken);
        }

        if (!_queueUrlResolved)
        {
            return null;
        }

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        ReceivedMessage? receivedMessage;

        try
        {
            if (_receivedMessages.Count == 0)
            {
                var messages = await _client.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = _queueUrl,
                    MaxNumberOfMessages = _amazonSqsOptions.MaxMessages,
                    WaitTimeSeconds = (int)_amazonSqsOptions.WaitTime.TotalSeconds,
                    VisibilityTimeout = (int)_visibilityTimeout.TotalSeconds
                }, cancellationToken).ConfigureAwait(false);

                foreach (var message in messages.Messages)
                {
                    var acknowledgementToken = new AcknowledgementToken(message.MessageId, message.Body, message.ReceiptHandle);

                    _acknowledgementTokens.TryAdd(acknowledgementToken.MessageId, acknowledgementToken);

                    _receivedMessages.Enqueue(new(new MemoryStream(Convert.FromBase64String(message.Body)), acknowledgementToken));
                }
            }

            receivedMessage = _receivedMessages.Count > 0 ? _receivedMessages.Dequeue() : null;
        }
        finally
        {
            _lock.Release();
        }

        if (receivedMessage != null)
        {
            LogMessage.MessageReceived(_logger, Uri.Uri.Scheme, Uri.TransportName);

            await _hopperOptions.MessageReceived.InvokeAsync(new(this, receivedMessage, pipeline), cancellationToken);
        }

        return receivedMessage;
    }

    public async ValueTask<bool> HasPendingAsync(CancellationToken cancellationToken = default)
    {
        if (!_initialized)
        {
            await GetQueueUrl(cancellationToken);
        }

        if (!_queueUrlResolved)
        {
            LogMessage.Operation(_logger, Uri.Uri.Scheme, Uri.TransportName, "[had-pending]");

            await _hopperOptions.TransportOperation.InvokeAsync(new(this, "[had-pending]", false), cancellationToken);

            return true;
        }

        LogMessage.Operation(_logger, Uri.Uri.Scheme, Uri.TransportName, "[has-pending/starting]");

        await _hopperOptions.TransportOperation.InvokeAsync(new(this, "[has-pending/starting]"), cancellationToken);

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        bool result;

        try
        {
            var response = _client.GetQueueAttributesAsync(new()
            {
                QueueUrl = _queueUrl,
                AttributeNames = _isEmptyAttributeNames
            }, cancellationToken).Result;

            result = response.ApproximateNumberOfMessages > 0 &&
                     response is { ApproximateNumberOfMessagesDelayed: > 0, ApproximateNumberOfMessagesNotVisible: > 0 };
        }
        finally
        {
            _lock.Release();
        }

        LogMessage.Operation(_logger, Uri.Uri.Scheme, Uri.TransportName, "[has-pending]");

        await _hopperOptions.TransportOperation.InvokeAsync(new(this, "[has-pending]", result), cancellationToken);

        return result;
    }

    public async Task ReleaseAsync(object acknowledgementToken, IPipeline pipeline, CancellationToken cancellationToken = default)
    {
        Guard.AgainstNull(acknowledgementToken);

        if (!_initialized)
        {
            await GetQueueUrl(cancellationToken);
        }

        GuardAgainstUnresolvedQueueUrl();

        if (!(acknowledgementToken is AcknowledgementToken data))
        {
            return;
        }

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            await _client.SendMessageAsync(new() { QueueUrl = _queueUrl, MessageBody = data.MessageBody }, cancellationToken).ConfigureAwait(false);
            await _client.DeleteMessageAsync(_queueUrl, data.ReceiptHandle, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _lock.Release();
        }

        _acknowledgementTokens.Remove(data.MessageId);

        LogMessage.MessageReleased(_logger, Uri.Uri.Scheme, Uri.TransportName);

        await _hopperOptions.MessageReleased.InvokeAsync(new(this, acknowledgementToken, pipeline), cancellationToken);
    }

    private async Task GetQueueUrl(CancellationToken cancellationToken)
    {
        try
        {
            _queueUrlResolved = false;

            try
            {
                _queueUrl = (await _client.GetQueueUrlAsync(new GetQueueUrlRequest { QueueName = Uri.TransportName }, cancellationToken).ConfigureAwait(false)).QueueUrl;
            }
            catch (AggregateException ex) when (ex.InnerException is TaskCanceledException)
            {
            }

            _queueUrlResolved = !string.IsNullOrWhiteSpace(_queueUrl);
        }
        catch (AmazonSQSException ex)
        {
            if (!ex.ErrorCode.Equals("QueueDoesNotExist", StringComparison.InvariantCulture) &&
                !ex.ErrorCode.Equals("AWS.SimpleQueueService.NonExistentQueue", StringComparison.InvariantCulture))
            {
                throw;
            }
        }

        _initialized = true;
    }

    private void GuardAgainstUnresolvedQueueUrl()
    {
        if (!_queueUrlResolved)
        {
            throw new ApplicationException(string.Format(Resources.QueueUrlNotResolvedException, Uri.TransportName));
        }
    }

    private void OnVisibilityTimeoutRenewalTimer(object? state)
    {
        _ = RenewVisibilityTimeoutsAsync();
    }

    private async Task RenewVisibilityTimeoutsAsync()
    {
        if (!_queueUrlResolved)
        {
            return;
        }

        if (!await _lock.WaitAsync(0).ConfigureAwait(false))
        {
            return;
        }

        try
        {
            foreach (var acknowledgementToken in _acknowledgementTokens.Values)
            {
                try
                {
                    await _client.ChangeMessageVisibilityAsync(_queueUrl, acknowledgementToken.ReceiptHandle, (int)_visibilityTimeout.TotalSeconds, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to renew the visibility timeout for message '{MessageId}' on transport '{TransportName}' ({Scheme}).", acknowledgementToken.MessageId, Uri.TransportName, Uri.Uri.Scheme);
                }
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    internal class AcknowledgementToken(string messageId, string messageBody, string receiptHandle)
    {
        public string MessageBody { get; } = messageBody;
        public string MessageId { get; } = messageId;
        public string ReceiptHandle { get; } = receiptHandle;
    }
}