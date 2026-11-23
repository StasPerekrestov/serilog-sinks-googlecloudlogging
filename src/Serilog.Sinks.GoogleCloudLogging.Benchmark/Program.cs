using BenchmarkDotNet.Running;
using Serilog.Sinks.GoogleCloudLogging.Benchmark;

_ = BenchmarkSwitcher.FromTypes([typeof(LogEventEmitBenchmark)]).Run(args);