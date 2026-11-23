using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Api;
using Google.Api.Gax.Grpc;
using Google.Cloud.Logging.Type;
using Google.Cloud.Logging.V2;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Formatting;

namespace Serilog.Sinks.GoogleCloudLogging;

public sealed class GoogleCloudLoggingSink : IBatchedLogEventSink
{
    private const int MaxEntryBytes = 250_000;
    private const int MaxRequestBytes = 9_000_000; // Leave headroom under the 10 MB write limit.
    private const int MaxInitialRequestCapacity = 1024;
    private const int MaxRetainedRenderChars = 40_000;
    private const int MaxCachedSourceNames = 1024;
    private const int MaxCachedSourceNameLength = 512;
    private static readonly int EntryTagSize = CodedOutputStream.ComputeTagSize(WriteLogEntriesRequest.EntriesFieldNumber);

    private readonly GoogleCloudLoggingSinkOptions _sinkOptions;
    private readonly LoggingServiceV2Client _client;
    private readonly MonitoredResource _resource;
    private readonly string _projectId;
    private readonly string _logName;
    private readonly string _logNamePrefix;
    private readonly LogFormatter _logFormatter;
    private readonly Struct? _serviceContext;

    // Serilog processes batches serially for each sink.
    private StringBuilder _stringBuilder = new();
    private Dictionary<string, string>? _sourceLogNames;
    private BatchFailureListener? _batchFailureListener;
    private int _acknowledgedEvents;
    private HashSet<int>? _reportedSkips;

    public GoogleCloudLoggingSink(GoogleCloudLoggingSinkOptions sinkOptions, ITextFormatter? textFormatter)
        : this(sinkOptions, textFormatter, MonitoredResourceBuilder.FromPlatform(), null)
    {
    }

    internal GoogleCloudLoggingSink(GoogleCloudLoggingSinkOptions sinkOptions, ITextFormatter? textFormatter,
        MonitoredResource resource, LoggingServiceV2Client? client)
    {
        _sinkOptions = sinkOptions;

        // retrieve current environment details automatically for GCE, GKE, GAE, or Cloud Run
        // set any user-provided resource type and labels which will override existing values from environment
        _resource = resource;
        _resource.Type = _sinkOptions.ResourceType ?? _resource.Type;
        foreach (var kvp in _sinkOptions.ResourceLabels)
            _resource.Labels[kvp.Key] = kvp.Value;

        _projectId = _sinkOptions.ProjectId ?? (_resource.Labels.TryGetValue("project_id", out var id) ? id : null) ?? "";
        if (_projectId.IsNullOrWhiteSpace())
            throw new ArgumentNullException(nameof(_projectId), "Project Id is not provided and could not be automatically discovered.");

        if (_sinkOptions.LogName.IsNullOrWhiteSpace())
            throw new ArgumentNullException(nameof(_sinkOptions.LogName), "Log Name is blank. Check assigned value or unset to use default.");

        _logName = LogFormatter.CreateLogName(_projectId, _sinkOptions.LogName)
            ?? throw new ArgumentException("Log Name contains no valid characters.", nameof(sinkOptions));
        _logNamePrefix = _logName.Substring(0, _logName.LastIndexOf('/') + 1);
        _logFormatter = new LogFormatter(textFormatter);
        GetMetadataSize(CreateRequest(0, _sinkOptions.Labels));

        // cache struct for service name and version contextual properties if available
        // these properties are required for any logged exceptions to automatically be picked up by cloud error reporting
        if (!_sinkOptions.ServiceName.IsNullOrWhiteSpace())
        {
            _serviceContext = new Struct();
            _serviceContext.Fields.Add("service", Value.ForString(_sinkOptions.ServiceName));
            if (!_sinkOptions.ServiceVersion.IsNullOrWhiteSpace())
                _serviceContext.Fields.Add("version", Value.ForString(_sinkOptions.ServiceVersion));
        }

        // logging client for google cloud apis
        _client = client ?? (_sinkOptions.GoogleCredentialJson.IsNullOrWhiteSpace()
            ? LoggingServiceV2Client.Create()
            : new LoggingServiceV2ClientBuilder { GoogleCredential = Google.Apis.Auth.OAuth2.CredentialFactory.FromJson<Google.Apis.Auth.OAuth2.GoogleCredential>(_sinkOptions.GoogleCredentialJson) }.Build());
    }

    public async Task EmitBatchAsync(IReadOnlyCollection<LogEvent> events)
    {
        var acknowledged = _acknowledgedEvents;
        var reportedSkips = _reportedSkips;
        if (events.Count == 0)
        {
            ResetRetryProgress();
            return;
        }

        var writer = new StringWriter(_stringBuilder);
        WriteLogEntriesRequest? request = null;
        var metadataSize = 0;
        var requestSize = 0;
        var processed = 0;

        try
        {
            foreach (var evnt in events)
            {
                var position = processed++;
                if (position < acknowledged || reportedSkips?.Contains(position) == true)
                    continue;

                LogEntry entry;
                int entrySize;
                try
                {
                    entry = CreateLogEntry(evnt, writer);
                    entrySize = entry.CalculateSize();
                }
                catch (Exception exception) when (exception is not OutOfMemoryException)
                {
                    writer = TrimRenderBuffer(writer);
                    ReportSkippedEntry(position, evnt, $"Could not prepare a Google Cloud Logging entry ({exception.GetType().Name}); skipping it.");
                    continue;
                }

                if (entrySize > MaxEntryBytes)
                {
                    writer = TrimRenderBuffer(writer);
                    ReportSkippedEntry(position, evnt, $"Log entry is too large for Google Cloud Logging: {entrySize} bytes (limit: {MaxEntryBytes}).");
                    continue;
                }

                if (request == null)
                {
                    request = CreateRequest(events.Count - processed + 1, _sinkOptions.Labels);
                    requestSize = metadataSize = GetMetadataSize(request);
                }

                var fieldSize = EntryTagSize + CodedOutputStream.ComputeLengthSize(entrySize) + entrySize;
                if (fieldSize > MaxRequestBytes - requestSize)
                {
                    if (fieldSize > MaxRequestBytes - metadataSize)
                    {
                        ReportSkippedEntry(position, evnt,
                            $"Log entry cannot fit with Google Cloud Logging request metadata: {fieldSize} + {metadataSize} bytes (limit: {MaxRequestBytes}).");
                        continue;
                    }

                    await _client.WriteLogEntriesAsync(request, CancellationToken.None).ConfigureAwait(false);
                    acknowledged = processed - 1;
                    request = CreateRequest(events.Count - processed + 1, request.Labels);
                    requestSize = metadataSize;
                }

                request.Entries.Add(entry);
                requestSize += fieldSize;
            }

            if (request is { Entries.Count: > 0 })
                await _client.WriteLogEntriesAsync(request, CancellationToken.None).ConfigureAwait(false);

            ResetRetryProgress();
        }
        catch
        {
            if (_batchFailureListener != null)
                _acknowledgedEvents = acknowledged;
            throw;
        }
        finally
        {
            writer.Dispose();
            if (_stringBuilder.Capacity > MaxRetainedRenderChars)
                _stringBuilder = new StringBuilder();
        }
    }

    private void ReportSkippedEntry(int position, LogEvent evnt, string reason)
    {
        if (_batchFailureListener is not { } listener)
        {
            SelfLog.WriteLine("{0}", reason);
            return;
        }

        // Retries keep positions, even when the same event object appears more than once.
        (_reportedSkips ??= new HashSet<int>()).Add(position);
        listener.ReportSkippedEntry(evnt, reason);
    }

    private StringWriter TrimRenderBuffer(StringWriter writer)
    {
        if (_stringBuilder.Capacity <= MaxRetainedRenderChars)
            return writer;

        var replacement = new StringWriter(_stringBuilder = new StringBuilder(), writer.FormatProvider);
        writer.Dispose();
        return replacement;
    }

    private WriteLogEntriesRequest CreateRequest(int capacity, IDictionary<string, string> labels)
    {
        var request = new WriteLogEntriesRequest
        {
            LogName = _logName,
            Resource = _resource,
            Labels = { labels }
        };
        request.Entries.Capacity = Math.Min(capacity, MaxInitialRequestCapacity);
        return request;
    }

    private static int GetMetadataSize(WriteLogEntriesRequest request)
    {
        var size = request.CalculateSize();
        if (size >= MaxRequestBytes - EntryTagSize - 1)
            throw new InvalidOperationException("Google Cloud Logging request metadata alone leaves no room for entries.");
        return size;
    }

    private LogEntry CreateLogEntry(LogEvent evnt, StringWriter writer)
    {
        var log = new LogEntry
        {
            Severity = TranslateSeverity(evnt.Level),
            Timestamp = Timestamp.FromDateTimeOffset(evnt.Timestamp)
        };

        // json output builds up a protobuf object to be serialized in cloud logging
        var jsonPayload = new Struct();
        jsonPayload.Fields["message"] = Value.ForString(_logFormatter.RenderEventMessage(evnt, writer));

        var propStruct = new Struct();
        jsonPayload.Fields["properties"] = Value.ForStruct(propStruct);
        if (evnt.Properties is Dictionary<string, LogEventPropertyValue> properties)
        {
            foreach (var property in properties)
                WriteEventProperty(log, propStruct, property.Key, property.Value);
        }
        else
        {
            foreach (var property in evnt.Properties)
                WriteEventProperty(log, propStruct, property.Key, property.Value);
        }

        if (_sinkOptions.UseLogCorrelation)
        {
            if (evnt.TraceId.ToString() is { Length: > 0 } traceId)
            {
                log.Trace = $"projects/{_projectId}/traces/{traceId}";
            }
            if (evnt.SpanId?.ToString() is { Length: > 0 } spanId)
            {
                log.SpanId = spanId;
            }
        }

        if (_serviceContext != null)
            jsonPayload.Fields["serviceContext"] = Value.ForStruct(_serviceContext);

        // format exception for Google Cloud Error Reporting
        // see: https://cloud.google.com/error-reporting/docs/formatting-error-messages
        if (evnt.Exception != null)
        {
            //uses exception property to use the same convention as the official NLog implementation target
            jsonPayload.Fields["exception"] = Value.ForString(evnt.Exception.ToString());
        }
        else if (evnt.Level >= LogEventLevel.Error)
        {
            //To log an error event that is a text message
            jsonPayload.Fields["@type"] = Value.ForString("type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent");
        }

        log.JsonPayload = jsonPayload;

        return log;
    }

    private void WriteEventProperty(LogEntry log, Struct properties, string key, LogEventPropertyValue value)
    {
        var field = _logFormatter.CreatePropertyValue(value);
        if (field != null)
            properties.Fields[key] = field;

        if (_sinkOptions.UseSourceContextAsLogName && key.Equals("SourceContext", StringComparison.OrdinalIgnoreCase))
            log.LogName = GetSourceLogName(GetString(value));

        if (_sinkOptions.UseLogCorrelation)
        {
            if (key.Equals("TraceSampled", StringComparison.OrdinalIgnoreCase))
                log.TraceSampled = GetBoolean(value);
        }

        static string GetString(LogEventPropertyValue v) => (v as ScalarValue)?.Value?.ToString() ?? "";
        static bool GetBoolean(LogEventPropertyValue v) => (v as ScalarValue)?.Value is true;
    }

    private string GetSourceLogName(string name)
    {
        if (name.Length > MaxCachedSourceNameLength)
            return CreateSourceLogName(name);

        var cache = _sourceLogNames ??= new Dictionary<string, string>(StringComparer.Ordinal);
        if (cache.TryGetValue(name, out var logName))
            return logName;

        logName = CreateSourceLogName(name);
        if (cache.Count < MaxCachedSourceNames)
            cache.Add(name, logName);
        return logName;
    }

    private string CreateSourceLogName(string name) =>
        LogFormatter.EscapeLogId(name) is { } logId ? _logNamePrefix + logId : "";

    private static LogSeverity TranslateSeverity(LogEventLevel level) => level switch
    {
        LogEventLevel.Verbose => LogSeverity.Debug,
        LogEventLevel.Debug => LogSeverity.Debug,
        LogEventLevel.Information => LogSeverity.Info,
        LogEventLevel.Warning => LogSeverity.Warning,
        LogEventLevel.Error => LogSeverity.Error,
        LogEventLevel.Fatal => LogSeverity.Critical,
        _ => LogSeverity.Default
    };

    public Task OnEmptyBatchAsync()
    {
        ResetRetryProgress();
        return Task.CompletedTask;
    }

    private void ResetRetryProgress()
    {
        _acknowledgedEvents = 0;
        _reportedSkips = null;
    }

    internal ILogEventSink CreateBatchingSink(BatchingOptions options)
    {
        var batcher = LoggerSinkConfiguration.CreateSink(writeTo => writeTo.Sink(this, options));
        if (batcher is not ISetLoggingFailureListener failureAwareBatcher)
            return batcher;

        var listener = new BatchFailureListener(this, batcher);
        failureAwareBatcher.SetFailureListener(listener);
        _batchFailureListener = listener;
        return listener;
    }

    private sealed class BatchFailureListener : ILogEventSink, ISetLoggingFailureListener, ILoggingFailureListener, IDisposable, IAsyncDisposable
    {
        private readonly GoogleCloudLoggingSink _sink;
        private readonly ILogEventSink _batcher;
        private ILoggingFailureListener _listener = SelfLog.FailureListener;

        public BatchFailureListener(GoogleCloudLoggingSink sink, ILogEventSink batcher)
        {
            _sink = sink;
            _batcher = batcher;
        }

        public void OnLoggingFailed(object sender, LoggingFailureKind kind, string message,
            IReadOnlyCollection<LogEvent>? events, Exception? exception)
        {
            var acknowledged = _sink._acknowledgedEvents;
            var reportedSkips = _sink._reportedSkips;
            // Serilog reuses its queue after reporting a drop.
            if (kind is LoggingFailureKind.Permanent or LoggingFailureKind.Final)
                _sink.ResetRetryProgress();

            if (events != null && (acknowledged > 0 || reportedSkips != null))
            {
                var pending = events.Skip(acknowledged);
                if (reportedSkips != null)
                    pending = pending.Where((_, index) => !reportedSkips.Contains(acknowledged + index));
                events = pending.ToArray();
            }

            ForwardFailure(sender, kind, message, events, exception);
        }

        public void ReportSkippedEntry(LogEvent evnt, string reason) =>
            ForwardFailure(_sink, LoggingFailureKind.Permanent, reason, [evnt], null);

        private void ForwardFailure(object sender, LoggingFailureKind kind, string message,
            IReadOnlyCollection<LogEvent>? events, Exception? exception)
        {
            try
            {
                Volatile.Read(ref _listener).OnLoggingFailed(sender, kind, message, events, exception);
            }
            catch (Exception listenerException) when (listenerException is not OutOfMemoryException)
            {
                try
                {
                    SelfLog.WriteLine("Failure listener threw {0} while handling: {1}", listenerException.GetType().Name, message);
                }
                catch (Exception selfLogException) when (selfLogException is not OutOfMemoryException)
                {
                    // SelfLog output can throw too.
                }
            }
        }

        public void SetFailureListener(ILoggingFailureListener listener) => Volatile.Write(ref _listener, listener);
        public void Emit(LogEvent logEvent) => _batcher.Emit(logEvent);
        public void Dispose() => (_batcher as IDisposable)?.Dispose();

        public ValueTask DisposeAsync()
        {
            if (_batcher is IAsyncDisposable asyncDisposable)
                return asyncDisposable.DisposeAsync();
            Dispose();
            return default;
        }
    }
}
