using System.Buffers;
using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using Google.Api;
using Google.Api.Gax.Grpc;
using Google.Cloud.Logging.V2;
using Google.Protobuf;
using Serilog.Events;
using Serilog.Parsing;
using JsonFormatter = Serilog.Formatting.Json.JsonFormatter;

namespace Serilog.Sinks.GoogleCloudLogging.Benchmark;

[MemoryDiagnoser]
public class LogEventEmitBenchmark
{
    public enum Workload
    {
        TextOnly,
        OneProperty,
        JsonFormatter,
        Structured,
        LargeBatch,
        SplitOnly,
        MediumMessage,
        SourceChurn
    }

    private const int EventsPerInvoke = 100;
    private GoogleCloudLoggingSink _sink = null!;
    private LogEvent[][][] _invocations = null!;
    private int _nextInvocation;

    [ParamsAllValues]
    public Workload Case { get; set; }

    [Params(1, 10, 100)]
    public int BatchSize { get; set; }

    [Params(false, true)]
    public bool Serialize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var formatter = Case == Workload.JsonFormatter ? new JsonFormatter() : null;
        _sink = new GoogleCloudLoggingSink(new GoogleCloudLoggingSinkOptions("project_test"), formatter,
            new MonitoredResource { Type = "global" }, new BenchmarkLoggingClient(Serialize));

        var timestamp = DateTimeOffset.UtcNow;
        var parser = new MessageTemplateParser();
        var template = parser.Parse(Case switch
        {
            Workload.TextOnly => "Worker started",
            Workload.Structured => "Handled {Method} {Path} with status {StatusCode}",
            Workload.LargeBatch => "{Body:l}",
            Workload.SplitOnly => "Payload received",
            Workload.MediumMessage => "Payload {Body:l}",
            _ => "Hello {@World}"
        });
        var traceId = Case == Workload.Structured ? ActivityTraceId.CreateRandom() : default;
        var spanId = Case == Workload.Structured ? ActivitySpanId.CreateRandom() : default;
        LogEventProperty[] properties = Case switch
        {
            Workload.TextOnly => [],
            // The body ends up in both message and properties: about 200 KB per entry.
            Workload.LargeBatch => [new("Body", new ScalarValue(new string('a', 100_000)))],
            Workload.SplitOnly => [new("Body", new ScalarValue(new string('a', 200_000)))],
            Workload.MediumMessage => [new("Body", new ScalarValue(new string('m', 20_000)))],
            Workload.Structured => [
                new("Method", new ScalarValue("GET")),
                new("Path", new ScalarValue("/api/v1/orders")),
                new("StatusCode", new ScalarValue(200)),
                new("TraceSampled", new ScalarValue(true)),
                new("SourceContext", new ScalarValue("Example.Orders.OrderController")),
                new("Order", new StructureValue([
                    new("id", new ScalarValue(1234)),
                    new("items", new SequenceValue(Enumerable.Range(0, 10).Select(i => new ScalarValue($"item-{i}")))),
                    new("region", new StructureValue([new("name", new ScalarValue("us-central1"))]))
                ]))
            ],
            _ => [new("World", new ScalarValue("Hello World!"))]
        };
        var eventCount = Case == Workload.SourceChurn ? 4_100 : EventsPerInvoke;
        _invocations = Enumerable.Range(0, eventCount).Select(i =>
            new LogEvent(timestamp.AddMilliseconds(i), LogEventLevel.Information, null, template,
                Case == Workload.SourceChurn
                    ? [.. properties, new("SourceContext", new ScalarValue($"App.Feature{i % 4_096}.Handler"))]
                    : properties,
                traceId, spanId))
            .Chunk(EventsPerInvoke)
            .Select(events => events.Chunk(BatchSize).ToArray())
            .ToArray();
    }

    [Benchmark(OperationsPerInvoke = EventsPerInvoke)]
    public Task EmitBatch()
    {
        var batches = _invocations[_nextInvocation++];
        if (_nextInvocation == _invocations.Length)
            _nextInvocation = 0;

        return batches.Length == 1 ? _sink.EmitBatchAsync(batches[0]) : EmitBatchesAsync(batches);
    }

    private async Task EmitBatchesAsync(LogEvent[][] batches)
    {
        foreach (var batch in batches)
            await _sink.EmitBatchAsync(batch);
    }

    private sealed class BenchmarkLoggingClient(bool serialize) : LoggingServiceV2Client
    {
        private static readonly Task<WriteLogEntriesResponse> CompletedWrite = Task.FromResult(new WriteLogEntriesResponse());
        private readonly ArrayBufferWriter<byte>? _buffer = serialize ? new() : null;

        public override Task<WriteLogEntriesResponse> WriteLogEntriesAsync(WriteLogEntriesRequest request, CallSettings callSettings)
        {
            if (_buffer != null)
            {
                _buffer.ResetWrittenCount();
                _buffer.GetSpan(request.CalculateSize());
                request.WriteTo(_buffer);
            }
            return CompletedWrite;
        }
    }
}
