using Amazon.SQS;
using Amazon.SQS.Model;
using Shuttle.Core.Contract;
using Shuttle.Core.Streams;

namespace Shuttle.Hopper.AmazonSqs;

public class AmazonSqsQueue(ServiceBusOptions serviceBusOptions, AmazonSqsOptions amazonSqsOptions, TransportUri uri)
    : ITransport, ICreateTransport, IDeleteTransport, IPurgeTransport, IDisposable
{
    private readonly Dictionary<string, AcknowledgementToken> _acknowledgementTokens = new();
    private readonly AmazonSqsOptions _amazonSqsOptions = Guard.AgainstNull(amazonSqsOptions);

    private readonly AmazonSQSClient _client = amazonSqsOptions.AwsCredentials == null
        ? new(amazonSqsOptions.AmazonSqsConfig ?? new AmazonSQSConfig())
        : new(amazonSqsOptions.AwsCredentials, amazonSqsOptions.AmazonSqsConfig ?? new AmazonSQSConfig());

    private readonly List<string> _isEmptyAttributeNames =
    [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesDelayed",
        "ApproximateNumberOfMessagesNotVisible"
    ];

    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly TimeSpan _operationTimeout = TimeSpan.FromSeconds(30);
    private readonly Queue<ReceivedMessage> _receivedMessages = new();
    private readonly ServiceBusOptions _serviceBusOptions = Guard.AgainstNull(serviceBusOptions);
    private bool _initialized;
    private string _queueUrl = string.Empty;
    private bool _queueUrlResolved;

    public async Task CreateAsync(CancellationToken cancellationToken = default)
    {
        await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[create/starting]"), cancellationToken);

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            await _client.CreateQueueAsync(new CreateQueueRequest { QueueName = Uri.TransportName }, cancellationToken).ConfigureAwait(false);
            await GetQueueUrl(cancellationToken);
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            _lock.Release();
        }

        await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[create/completed]"), cancellationToken);
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

        await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[drop/starting]"), cancellationToken);

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            await _client.DeleteQueueAsync(new DeleteQueueRequest { QueueUrl = _queueUrl }, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[drop/cancelled]"), cancellationToken);
        }
        finally
        {
            _lock.Release();
        }

        await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[drop/completed]"), cancellationToken);
    }

    public void Dispose()
    {
        if (!_queueUrlResolved)
        {
            return;
        }

        _lock.Wait(CancellationToken.None);

        try
        {
            foreach (var acknowledgementToken in _acknowledgementTokens.Values)
            {
                try
                {
                    _client.SendMessageAsync(new() { QueueUrl = _queueUrl, MessageBody = acknowledgementToken.MessageBody }).Wait(_operationTimeout);
                    _client.DeleteMessageAsync(_queueUrl, acknowledgementToken.ReceiptHandle).Wait(_operationTimeout);
                }
                catch (OperationCanceledException)
                {
                }
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

        await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[purge/starting]"), cancellationToken);

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            await _client.PurgeQueueAsync(new PurgeQueueRequest { QueueUrl = _queueUrl }, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[purge/cancelled]"), cancellationToken);
        }
        finally
        {
            _lock.Release();
        }

        await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[purge/completed]"), cancellationToken);
    }

    public TransportUri Uri { get; } = Guard.AgainstNull(uri);

    public async Task AcknowledgeAsync(object acknowledgementToken, CancellationToken cancellationToken = default)
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

            await _serviceBusOptions.MessageAcknowledged.InvokeAsync(new(this, acknowledgementToken), cancellationToken);
        }
        catch (OperationCanceledException)
        {
            await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[acknowledge/cancelled]"), cancellationToken);
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task SendAsync(TransportMessage message, Stream stream, CancellationToken cancellationToken = default)
    {
        Guard.AgainstNull(message);
        Guard.AgainstNull(stream);

        if (!_initialized)
        {
            await GetQueueUrl(cancellationToken);
        }

        GuardAgainstUnresolvedQueueUrl();

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            await _client.SendMessageAsync(new() { QueueUrl = _queueUrl, MessageBody = Convert.ToBase64String(await stream.ToBytesAsync()) }, cancellationToken).ConfigureAwait(false);

            await _serviceBusOptions.MessageSent.InvokeAsync(new(this, message, stream), cancellationToken);
        }
        catch (OperationCanceledException)
        {
            await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[enqueue/cancelled]"), cancellationToken);
        }
        finally
        {
            _lock.Release();
        }
    }

    public TransportType Type => TransportType.Queue;

    public async Task<ReceivedMessage?> ReceiveAsync(CancellationToken cancellationToken = default)
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

        try
        {
            if (_receivedMessages.Count == 0)
            {
                var messages = await _client.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = _queueUrl,
                    MaxNumberOfMessages = _amazonSqsOptions.MaxMessages,
                    WaitTimeSeconds = (int)_amazonSqsOptions.WaitTime.TotalSeconds
                }, cancellationToken).ConfigureAwait(false);

                foreach (var message in messages.Messages)
                {
                    var acknowledgementToken = new AcknowledgementToken(message.MessageId, message.Body, message.ReceiptHandle);

                    _acknowledgementTokens.TryAdd(acknowledgementToken.MessageId, acknowledgementToken);

                    _receivedMessages.Enqueue(new(new MemoryStream(Convert.FromBase64String(message.Body)), acknowledgementToken));
                }
            }

            var receivedMessage = _receivedMessages.Count > 0 ? _receivedMessages.Dequeue() : null;

            if (receivedMessage != null)
            {
                await _serviceBusOptions.MessageReceived.InvokeAsync(new(this, receivedMessage), cancellationToken);
            }

            return receivedMessage;
        }
        catch (OperationCanceledException)
        {
            await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[receive/cancelled]"), cancellationToken);
        }
        finally
        {
            _lock.Release();
        }

        return null;
    }

    public async ValueTask<bool> HasPendingAsync(CancellationToken cancellationToken = default)
    {
        if (!_initialized)
        {
            await GetQueueUrl(cancellationToken);
        }

        if (!_queueUrlResolved)
        {
            await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[had-pending]", false), cancellationToken);

            return true;
        }

        await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[has-pending/starting]"), cancellationToken);

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            var response = _client.GetQueueAttributesAsync(new()
            {
                QueueUrl = _queueUrl,
                AttributeNames = _isEmptyAttributeNames
            }, cancellationToken).Result;

            var result =
                response.ApproximateNumberOfMessages > 0 &&
                response is { ApproximateNumberOfMessagesDelayed: > 0, ApproximateNumberOfMessagesNotVisible: > 0 };

            await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[has-pending]", result), cancellationToken);

            return result;
        }
        catch (OperationCanceledException)
        {
            await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[has-pending/cancelled]", false), cancellationToken);
        }
        finally
        {
            _lock.Release();
        }

        return false;
    }

    public async Task ReleaseAsync(object acknowledgementToken, CancellationToken cancellationToken = default)
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

            await _serviceBusOptions.MessageReleased.InvokeAsync(new(this, acknowledgementToken), cancellationToken);
        }
        catch (OperationCanceledException)
        {
            await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[release/cancelled]"), cancellationToken);
        }
        finally
        {
            _lock.Release();
        }

        _acknowledgementTokens.Remove(data.MessageId);
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
            catch (OperationCanceledException)
            {
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

    internal class AcknowledgementToken(string messageId, string messageBody, string receiptHandle)
    {
        public string MessageBody { get; } = messageBody;
        public string MessageId { get; } = messageId;
        public string ReceiptHandle { get; } = receiptHandle;
    }
}