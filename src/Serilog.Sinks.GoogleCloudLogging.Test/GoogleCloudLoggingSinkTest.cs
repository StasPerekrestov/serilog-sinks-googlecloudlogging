using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Google.Api;
using Google.Api.Gax.Grpc;
using Google.Cloud.Logging.V2;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Parsing;
using Xunit;

namespace Serilog.Sinks.GoogleCloudLogging.Test;

[Collection("SelfLog")]
public class GoogleCloudLoggingSinkTest
{
    private static readonly DateTimeOffset Timestamp = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly MessageTemplate LargeTemplate = new MessageTemplateParser().Parse(new string('a', 230_000));
    private static readonly MessageTemplate OversizedTemplate = new MessageTemplateParser().Parse(new string('\u00e9', 130_000));
    private static readonly MessageTemplate MediumTemplate = new MessageTemplateParser().Parse("Medium test message");

    [Fact]
    public async Task WillScopeDefaultAndSourceLogNamesToTheSinkProject()
    {
        var first = new RecordingClient();
        var second = new RecordingClient();
        var evnt = CreateEvent(MessageTemplate.Empty, properties: [new("SourceContext", new ScalarValue("Orders/Api"))]);

        await Task.WhenAll(
            CreateSink(first, "project-a").EmitBatchAsync([evnt]),
            CreateSink(second, "project-b").EmitBatchAsync([evnt]));

        var requestA = Assert.Single(first.Requests);
        var requestB = Assert.Single(second.Requests);
        Assert.Equal("projects/project-a/logs/Default", requestA.LogName);
        Assert.Equal("projects/project-b/logs/Default", requestB.LogName);
        Assert.Equal("projects/project-a/logs/Orders%2FApi", Assert.Single(requestA.Entries).LogName);
        Assert.Equal("projects/project-b/logs/Orders%2FApi", Assert.Single(requestB.Entries).LogName);
    }

    [Theory]
    [InlineData("App.Worker_1-v2", "App.Worker_1-v2")]
    [InlineData("App!Worker/Api", "AppWorker%2FApi")]
    public async Task WillPreserveEscapingForSourceNames(string source, string logId)
    {
        var client = new RecordingClient();
        var sink = new GoogleCloudLoggingSink(new GoogleCloudLoggingSinkOptions("project_test", logName: "Default/Events"),
            null, new MonitoredResource { Type = "global" }, client);
        var evnt = CreateEvent(MessageTemplate.Empty, properties: [new("SourceContext", new ScalarValue(source))]);

        await sink.EmitBatchAsync([evnt]);

        var request = Assert.Single(client.Requests);
        Assert.Equal("projects/project_test/logs/Default%2FEvents", request.LogName);
        Assert.Equal($"projects/project_test/logs/{logId}", Assert.Single(request.Entries).LogName);
    }

    [Fact]
    public async Task WillKeepCacheHitsWithoutRetainingEverySourceName()
    {
        var client = new RecordingClient();
        var sink = CreateSink(client);
        LogEvent FromSource(string name) =>
            CreateEvent(MessageTemplate.Empty, properties: [new("SourceContext", new ScalarValue(name))]);
        var hot = FromSource("HotSource");
        var longName = FromSource(new string('x', 5000));
        var churn = Enumerable.Range(0, 2000).Select(i => FromSource($"Source{i}")).ToArray();

        await sink.EmitBatchAsync([hot, longName]);
        await sink.EmitBatchAsync(churn);
        await sink.EmitBatchAsync([hot, longName, churn[^1]]);

        Assert.Same(client.Requests[0].Entries[0].LogName, client.Requests[2].Entries[0].LogName);
        Assert.Equal(client.Requests[0].Entries[1].LogName, client.Requests[2].Entries[1].LogName);
        Assert.NotSame(client.Requests[0].Entries[1].LogName, client.Requests[2].Entries[1].LogName);
        Assert.Equal(client.Requests[1].Entries[^1].LogName, client.Requests[2].Entries[2].LogName);
        Assert.NotSame(client.Requests[1].Entries[^1].LogName, client.Requests[2].Entries[2].LogName);
    }

    [Fact]
    public async Task WillSplitRequestsAndPreserveEntryOrderAndRequestMetadata()
    {
        var client = new RecordingClient();
        var label = new string('b', 25_000);
        var labels = Enumerable.Range(0, 40).ToDictionary(i => $"label{i}", _ => label);
        labels["app"] = "orders";
        var options = new GoogleCloudLoggingSinkOptions("project_test", labels: labels,
            resourceLabels: new() { ["project_id"] = "project_test" }, serviceName: "web");
        var sink = new GoogleCloudLoggingSink(options, null, new MonitoredResource { Type = "global" }, client);
        var events = Enumerable.Range(0, 100)
            .Select(i => CreateEvent(i is 0 or 35 or 36 or 99 ? OversizedTemplate : LargeTemplate, i)).ToArray();

        await sink.EmitBatchAsync(events);

        Assert.True(client.Requests.Count > 1);
        Assert.All(client.Requests, request =>
        {
            Assert.NotEmpty(request.Entries);
            Assert.InRange(request.CalculateSize(), 1, 9_000_000);
            Assert.Equal("projects/project_test/logs/Default", request.LogName);
            Assert.Equal("orders", request.Labels["app"]);
            Assert.Equal("global", request.Resource.Type);
            Assert.Equal("project_test", request.Resource.Labels["project_id"]);
            Assert.False(request.PartialSuccess);
        });
        var entries = client.Requests.SelectMany(request => request.Entries).ToArray();
        Assert.Equal(events.Where(evnt => evnt.MessageTemplate == LargeTemplate).Select(evnt => evnt.Timestamp),
            entries.Select(entry => entry.Timestamp.ToDateTimeOffset()));
        Assert.Equal("web", entries[0].JsonPayload.Fields["serviceContext"].StructValue.Fields["service"].StringValue);
    }

    [Fact]
    public async Task WillReportAnEntryThatCannotFitAlongsideMetadata()
    {
        var client = new RecordingClient();
        var options = new GoogleCloudLoggingSinkOptions("project_test", resourceLabels: new()
        {
            ["probe"] = new string('m', 8_850_000)
        });
        var sink = new GoogleCloudLoggingSink(options, null, new MonitoredResource { Type = "global" }, client);
        var large = CreateEvent(LargeTemplate);
        var small = CreateEvent(MessageTemplate.Empty, 1);
        var failures = new List<(LoggingFailureKind Kind, string Message, IReadOnlyCollection<LogEvent> Events)>();
        var listener = new TestFailureListener
        {
            OnFailure = (kind, message, events, _) => failures.Add((kind, message, events))
        };

        await using (var logger = new LoggerConfiguration().WriteTo.Fallible(
            writeTo => writeTo.Sink(sink.CreateBatchingSink(new BatchingOptions())), listener).CreateLogger())
        {
            logger.Write(large);
            logger.Write(small);
        }

        var request = Assert.Single(client.Requests);
        Assert.InRange(request.CalculateSize(), 1, 9_000_000);
        Assert.Equal(small.Timestamp, Assert.Single(request.Entries).Timestamp.ToDateTimeOffset());
        var failure = Assert.Single(failures);
        Assert.Equal(LoggingFailureKind.Permanent, failure.Kind);
        Assert.Contains("cannot fit with Google Cloud Logging request metadata", failure.Message);
        Assert.Same(large, Assert.Single(failure.Events));
    }

    [Fact]
    public void WillRejectMetadataThatLeavesNoRoomForEntriesAtStartup()
    {
        var options = new GoogleCloudLoggingSinkOptions("project_test", resourceLabels: new()
        {
            ["probe"] = new string('m', 9_000_000)
        });
        var error = Assert.Throws<InvalidOperationException>(() =>
            new GoogleCloudLoggingSink(options, null, new MonitoredResource { Type = "global" }, new RecordingClient()));

        Assert.Contains("metadata alone", error.Message);
    }

    [Fact]
    public async Task WillUseTheDefaultLogNameWhenSourceContextHasNoValidCharacters()
    {
        var client = new RecordingClient();
        var evnt = CreateEvent(MessageTemplate.Empty, properties: [new("SourceContext", new ScalarValue("!!!"))]);

        await CreateSink(client).EmitBatchAsync([evnt]);

        var request = Assert.Single(client.Requests);
        var entry = Assert.Single(request.Entries);
        Assert.Equal("projects/project_test/logs/Default", request.LogName);
        Assert.Empty(entry.LogName);
        Assert.Equal("!!!", entry.JsonPayload.Fields["properties"].StructValue.Fields["SourceContext"].StringValue);
    }

    [Theory]
    [InlineData(nameof(ArgumentException))]
    [InlineData(nameof(InvalidOperationException))]
    [InlineData(nameof(OperationCanceledException))]
    public async Task WillSkipAnUnformattableEntryAndKeepItsHealthyNeighbors(string exceptionType)
    {
        var client = new RecordingClient();
        var sink = CreateSink(client);
        LogEventPropertyValue value = exceptionType != nameof(ArgumentException)
            ? new ScalarValue(new ThrowingScalar(exceptionType == nameof(OperationCanceledException)))
            : new DictionaryValue(
            [
                new(new ScalarValue(1), new ScalarValue("first")),
                new(new ScalarValue("1"), new ScalarValue("second"))
            ]);
        var before = CreateEvent(MessageTemplate.Empty, 0);
        var bad = CreateEvent(MessageTemplate.Empty, 1, [new("Payload", value)]);
        var after = CreateEvent(MessageTemplate.Empty, 2);
        var diagnostics = new ConcurrentQueue<string>();
        SelfLog.Enable(diagnostics.Enqueue);
        try
        {
            await sink.EmitBatchAsync([before, bad, after]);
        }
        finally
        {
            SelfLog.Disable();
        }

        var request = Assert.Single(client.Requests);
        Assert.Equal(new[] { before.Timestamp, after.Timestamp }, request.Entries.Select(entry => entry.Timestamp.ToDateTimeOffset()));
        var diagnostic = Assert.Single(diagnostics, line => line.Contains("Could not prepare a Google Cloud Logging entry"));
        Assert.Contains(exceptionType, diagnostic);
        Assert.DoesNotContain("sensitive scalar value", diagnostic);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task WillFallbackForSkippedEntriesWithoutRepeatingThemOnABatchDrop(bool drop, bool fallbackThrows)
    {
        var malformed = CreateEvent(MessageTemplate.Empty, 1, [new("Payload", new ScalarValue(new ThrowingScalar()))]);
        var oversized = CreateEvent(OversizedTemplate, 2);
        var healthy = CreateEvent(MessageTemplate.Empty, 3);
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fallback = new RecordingSink
        {
            OnEmit = logEvent =>
            {
                if (logEvent == healthy)
                    completed.TrySetResult();
                else if (fallbackThrows)
                    throw new IOException("Test fallback failure.");
            }
        };
        var client = new RecordingClient { FailOnWrite = drop ? 1 : 0 };
        var sink = CreateSink(client);
        var diagnostics = new ConcurrentQueue<string>();
        SelfLog.Enable(diagnostics.Enqueue);
        try
        {
            await using var logger = new LoggerConfiguration().WriteTo.FallbackChain(
                writeTo => writeTo.Sink(sink.CreateBatchingSink(new BatchingOptions
                {
                    BatchSizeLimit = 3,
                    BufferingTimeLimit = TimeSpan.FromMilliseconds(100),
                    EagerlyEmitFirstEvent = false,
                    RetryTimeLimit = TimeSpan.Zero
                })),
                writeTo => writeTo.Sink(fallback)).CreateLogger();
            logger.Write(malformed);
            logger.Write(oversized);
            logger.Write(healthy);
            if (drop)
                await completed.Task.WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally
        {
            SelfLog.Disable();
        }

        if (fallbackThrows)
            Assert.Contains(diagnostics, line => line.Contains("Failure listener threw IOException") &&
                line.Contains("Could not prepare a Google Cloud Logging entry"));

        if (drop)
        {
            Assert.Empty(client.Requests);
            Assert.Equal([malformed, oversized, healthy], fallback.Events);
        }
        else
        {
            Assert.Equal(healthy.Timestamp, Assert.Single(Assert.Single(client.Requests).Entries).Timestamp.ToDateTimeOffset());
            Assert.Equal([malformed, oversized], fallback.Events);
        }
    }

    [Theory]
    [InlineData(LoggingFailureKind.Temporary)]
    [InlineData(LoggingFailureKind.Permanent)]
    public async Task WillContinueWritingAfterABatchFailureListenerThrows(LoggingFailureKind failureKind)
    {
        var client = new RecordingClient { FailOnWrite = 1 };
        var sink = CreateSink(client).CreateBatchingSink(new BatchingOptions
        {
            BatchSizeLimit = 1,
            RetryTimeLimit = TimeSpan.Zero
        });
        var failed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        void Throw()
        {
            failed.TrySetResult();
            throw new IOException("sensitive listener failure");
        }

        var config = new LoggerConfiguration();
        if (failureKind == LoggingFailureKind.Permanent)
            config.WriteTo.FallbackChain(writeTo => writeTo.Sink(sink),
                writeTo => writeTo.Sink(new RecordingSink { OnEmit = _ => Throw() }));
        else
            config.WriteTo.Fallible(writeTo => writeTo.Sink(sink), new TestFailureListener
            {
                OnFailure = (kind, _, _, _) =>
                {
                    if (kind == failureKind)
                        Throw();
                }
            });

        var diagnostics = new ConcurrentQueue<string>();
        var later = CreateEvent(MessageTemplate.Empty, 1);
        SelfLog.Enable(diagnostics.Enqueue);
        try
        {
            await using var logger = config.CreateLogger();
            logger.Write(CreateEvent(MessageTemplate.Empty));
            await failed.Task.WaitAsync(TimeSpan.FromSeconds(10));
            logger.Write(later);
        }
        finally
        {
            SelfLog.Disable();
        }

        var diagnostic = Assert.Single(diagnostics, line => line.Contains("Failure listener threw IOException"));
        Assert.DoesNotContain("sensitive listener failure", diagnostic);
        var sent = Assert.Single(Assert.Single(client.Requests).Entries);
        Assert.Equal(later.Timestamp, sent.Timestamp.ToDateTimeOffset());
    }

    [Fact]
    public async Task WillContinueWritingWhenSelfLogOutputIsDisposed()
    {
        var client = new RecordingClient { FailOnWrite = 1 };
        var sink = CreateSink(client).CreateBatchingSink(new BatchingOptions
        {
            BatchSizeLimit = 1,
            RetryTimeLimit = TimeSpan.Zero
        });
        var diagnosticAttempted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writer = new StringWriter();
        writer.Dispose();
        var later = CreateEvent(MessageTemplate.Empty, 1);

        SelfLog.Enable(message =>
        {
            diagnosticAttempted.TrySetResult();
            writer.WriteLine(message);
        });
        try
        {
            await using var logger = new LoggerConfiguration().WriteTo.Sink(sink).CreateLogger();
            logger.Write(CreateEvent(MessageTemplate.Empty));
            await diagnosticAttempted.Task.WaitAsync(TimeSpan.FromSeconds(10));
            logger.Write(later);
        }
        finally
        {
            SelfLog.Disable();
        }

        var sent = Assert.Single(Assert.Single(client.Requests).Entries);
        Assert.Equal(later.Timestamp, sent.Timestamp.ToDateTimeOffset());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WillResumeRetriesButNotSkipReenqueuedEventsAfterADrop(bool drop)
    {
        var validEvents = Enumerable.Range(0, 55).Select(i => CreateEvent(LargeTemplate, i)).ToArray();
        var scalar = new ThrowingScalar();
        var malformed = CreateEvent(MessageTemplate.Empty, -1, [new("Payload", new ScalarValue(scalar))]);
        LogEvent[] events = [malformed, ..validEvents, malformed];
        var sentinel = CreateEvent(MessageTemplate.Empty, 99);
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new RecordingClient
        {
            FailOnWrite = 2,
            OnWrite = request =>
            {
                if (request.Entries.Any(entry => entry.Timestamp.ToDateTimeOffset() == sentinel.Timestamp))
                    completed.TrySetResult();
            }
        };
        var sink = CreateSink(client);
        var listener = new TestFailureListener();
        var skipped = new List<IReadOnlyCollection<LogEvent>>();
        await using var logger = new LoggerConfiguration().WriteTo.Fallible(writeTo =>
            writeTo.Sink(sink.CreateBatchingSink(new BatchingOptions
            {
                BatchSizeLimit = events.Length + 1,
                BufferingTimeLimit = TimeSpan.FromMilliseconds(100),
                EagerlyEmitFirstEvent = false,
                RetryTimeLimit = drop ? TimeSpan.Zero : TimeSpan.FromMinutes(1)
            })), listener).CreateLogger();
        LogEvent[] failedEvents = null;
        listener.OnFailure = (kind, _, failed, exception) =>
        {
            if (kind == LoggingFailureKind.Permanent && exception == null)
            {
                skipped.Add(failed);
                return;
            }

            if (kind != (drop ? LoggingFailureKind.Permanent : LoggingFailureKind.Temporary))
                return;

            failedEvents = failed.ToArray();
            if (drop)
                foreach (var evnt in events)
                    logger.Write(evnt);
            logger.Write(sentinel);
        };
        foreach (var evnt in events)
            logger.Write(evnt);

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(validEvents.Skip(client.Requests[0].Entries.Count), failedEvents);
        Assert.Equal(drop ? 4 : 2, skipped.Count);
        Assert.All(skipped, failure => Assert.Same(malformed, Assert.Single(failure)));
        Assert.Equal(skipped.Count, scalar.Calls);
        var acknowledged = drop ? client.Requests[0].Entries.Count : 0;
        var expected = validEvents.Take(acknowledged).Concat(validEvents).Append(sentinel).Select(evnt => evnt.Timestamp);
        Assert.Equal(expected, client.Requests.SelectMany(request => request.Entries).Select(entry => entry.Timestamp.ToDateTimeOffset()));
        if (!drop)
            Assert.Contains(client.Requests[^1].Entries, entry => entry.Timestamp.ToDateTimeOffset() == validEvents[^1].Timestamp);
    }

    [Fact]
    public async Task WillAwaitPendingWritesWithoutBlockingAsyncDisposal()
    {
        var writeStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseWrite = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new RecordingClient
        {
            BeforeWriteAsync = () =>
            {
                writeStarted.SetResult();
                return releaseWrite.Task;
            }
        };
        var logger = new LoggerConfiguration().WriteTo
            .Sink(CreateSink(client).CreateBatchingSink(new BatchingOptions())).CreateLogger();
        logger.Write(CreateEvent(MessageTemplate.Empty));

        var disposalReturned = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var disposal = Task.Run(async () =>
        {
            var flush = logger.DisposeAsync();
            disposalReturned.SetResult(flush.IsCompleted);
            await flush;
        });

        try
        {
            await writeStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.False(await disposalReturned.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        }
        finally
        {
            releaseWrite.SetResult();
            await disposal.WaitAsync(TimeSpan.FromSeconds(10));
        }

        Assert.Single(client.Requests);
    }

    [Fact]
    public async Task WillNotAssumeThatDirectCallsReuseThePreviousBatch()
    {
        var client = new RecordingClient { FailOnWrite = 2 };
        var sink = CreateSink(client);
        var events = Enumerable.Range(0, 55).Select(i => CreateEvent(LargeTemplate, i)).ToArray();

        await Assert.ThrowsAsync<IOException>(() => sink.EmitBatchAsync(events));
        await sink.EmitBatchAsync(events);

        Assert.Equal(events.Select(evnt => evnt.Timestamp), client.Requests.Skip(1)
            .SelectMany(request => request.Entries).Select(entry => entry.Timestamp.ToDateTimeOffset()));
    }

    [Fact]
    public async Task WillNotReuseAnOversizedRenderBufferWithinOrAcrossBatches()
    {
        var client = new RecordingClient();
        var formatter = new TrackingFormatter();
        var sink = CreateSink(client, formatter: formatter);

        await sink.EmitBatchAsync([CreateEvent(LargeTemplate), CreateEvent(MessageTemplate.Empty), CreateEvent(LargeTemplate)]);
        await sink.EmitBatchAsync([CreateEvent(MessageTemplate.Empty)]);

        Assert.Equal(4, formatter.Capacities.Count);
        Assert.All(formatter.Capacities, capacity => Assert.InRange(capacity, 0, 40_000));
        var entries = client.Requests.SelectMany(request => request.Entries).ToArray();
        Assert.Equal(2, entries.Length);
        Assert.All(entries, entry => Assert.Equal("small", entry.JsonPayload.Fields["message"].StringValue));
    }

    [Fact]
    public async Task WillReuseAMediumRenderBufferAcrossBatches()
    {
        var client = new RecordingClient();
        var formatter = new TrackingFormatter();
        var sink = CreateSink(client, formatter: formatter);
        var evnt = CreateEvent(MediumTemplate);

        await sink.EmitBatchAsync([evnt]);
        await sink.EmitBatchAsync([evnt]);

        Assert.InRange(formatter.Capacities[1], 20_000, 40_000);
    }

    [Fact]
    public async Task WillReportAnOversizedEntryWithoutLoggingItsPayload()
    {
        var client = new RecordingClient();
        var sink = CreateSink(client, formatter: new TrackingFormatter());
        var diagnostics = new ConcurrentQueue<string>();
        SelfLog.Enable(diagnostics.Enqueue);
        try
        {
            await sink.EmitBatchAsync([CreateEvent(LargeTemplate)]);
        }
        finally
        {
            SelfLog.Disable();
        }

        Assert.Empty(client.Requests);
        var diagnostic = Assert.Single(diagnostics, line => line.Contains("Log entry is too large for Google Cloud Logging"));
        Assert.Contains("bytes (limit: 250000)", diagnostic);
        Assert.InRange(diagnostic.Length, 1, 200);
    }

    private static GoogleCloudLoggingSink CreateSink(RecordingClient client, string projectId = "project_test",
        ITextFormatter formatter = null) =>
        new(new GoogleCloudLoggingSinkOptions(projectId), formatter, new MonitoredResource { Type = "global" }, client);

    private static LogEvent CreateEvent(MessageTemplate template, int milliseconds = 0, LogEventProperty[] properties = null) =>
        new(Timestamp.AddMilliseconds(milliseconds), LogEventLevel.Information, null, template, properties ?? []);

    private sealed class ThrowingScalar(bool cancels = false)
    {
        public int Calls { get; private set; }

        public override string ToString()
        {
            Calls++;
            throw cancels
                ? new OperationCanceledException("sensitive scalar value")
                : new InvalidOperationException("sensitive scalar value");
        }
    }

    private sealed class TrackingFormatter : ITextFormatter
    {
        private static readonly string Oversized = new('x', 500_000);
        private static readonly string Medium = new('m', 20_000);
        public List<int> Capacities { get; } = new();

        public void Format(LogEvent logEvent, TextWriter output)
        {
            Capacities.Add(((StringWriter)output).GetStringBuilder().Capacity);
            output.Write(logEvent.MessageTemplate == LargeTemplate ? Oversized :
                logEvent.MessageTemplate == MediumTemplate ? Medium : "small");
        }
    }

    private sealed class TestFailureListener : ILoggingFailureListener
    {
        public Action<LoggingFailureKind, string, IReadOnlyCollection<LogEvent>, Exception> OnFailure { get; set; }

        public void OnLoggingFailed(object sender, LoggingFailureKind kind, string message, IReadOnlyCollection<LogEvent> events,
            Exception exception) => OnFailure(kind, message, events, exception);
    }

    private sealed class RecordingSink : ILogEventSink
    {
        public Action<LogEvent> OnEmit { get; init; }
        public List<LogEvent> Events { get; } = new();

        public void Emit(LogEvent logEvent)
        {
            Events.Add(logEvent);
            OnEmit?.Invoke(logEvent);
        }
    }

    private sealed class RecordingClient : LoggingServiceV2Client
    {
        private int _writes;
        public int FailOnWrite { get; init; }
        public Func<Task> BeforeWriteAsync { get; init; }
        public Action<WriteLogEntriesRequest> OnWrite { get; init; }
        public List<WriteLogEntriesRequest> Requests { get; } = new();

        public override async Task<WriteLogEntriesResponse> WriteLogEntriesAsync(WriteLogEntriesRequest request, CallSettings callSettings)
        {
            await Task.Yield();
            if (++_writes == FailOnWrite)
                throw new IOException("Test write failure.");

            if (BeforeWriteAsync != null)
                await BeforeWriteAsync();

            Requests.Add(request);
            OnWrite?.Invoke(request);
            return new WriteLogEntriesResponse();
        }
    }
}
