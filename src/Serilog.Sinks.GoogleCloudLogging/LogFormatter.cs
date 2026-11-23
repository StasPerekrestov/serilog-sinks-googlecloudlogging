using System;
using System.IO;
using System.Text.Encodings.Web;
using System.Text.RegularExpressions;
using Google.Cloud.Logging.V2;
using Google.Protobuf.WellKnownTypes;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Parsing;
#if NET8_0_OR_GREATER
using System.Buffers;
#endif

namespace Serilog.Sinks.GoogleCloudLogging;

internal partial class LogFormatter
{
    private readonly ITextFormatter? _textFormatter;

    #if NET8_0_OR_GREATER
    private static readonly SearchValues<char> UnescapedLogIdChars =
        SearchValues.Create("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz._-");

    [GeneratedRegex("[^0-9A-Z._/-]+", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.CultureInvariant)]
    private static partial Regex LogNameUnsafeChars();
    #else 
      private static readonly Regex LogNameUnsafeCharsRegex = new("[^0-9A-Z._/-]+", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.CultureInvariant);
      private static  Regex LogNameUnsafeChars() => LogNameUnsafeCharsRegex;
    #endif

    public LogFormatter(ITextFormatter? textFormatter)
    {
        _textFormatter = textFormatter;
    }

    public string RenderEventMessage(LogEvent e, StringWriter writer)
    {
        if (_textFormatter == null && e.Exception == null && e.MessageTemplate.Tokens is MessageTemplateToken[] tokens)
        {
            if (tokens.Length == 1)
            {
                if (tokens[0] is TextToken text)
                    return text.Text;

                if (tokens[0] is PropertyToken property && GetLiteralString(e, property) is { } literal)
                    return literal;
            }
            else if (tokens.Length == 2 && tokens[0] is TextToken prefix && tokens[1] is PropertyToken tail &&
                GetLiteralString(e, tail) is { } suffix)
            {
                return prefix.Text + suffix;
            }
        }

        writer.GetStringBuilder().Clear();

        // use formatter if provided
        if (_textFormatter != null)
        {
            _textFormatter.Format(e, writer);
        }
        else
        {
            try
            {
                e.RenderMessage(writer);
            }
            catch (FormatException)
            {
                Debugging.SelfLog.WriteLine("Could not render a log message due to an invalid format; using its message template.");
                writer.GetStringBuilder().Clear();
                writer.Write(e.MessageTemplate.Text);
            }

            if (e.Exception != null)
            {
                // check the current message length and add new line before exception stack trace if needed
                if (writer.GetStringBuilder().Length > 0)
                    writer.WriteLine();

                //Exception message stacktrace goes as a separate exception property
                writer.Write(e.Exception.Message);
            }
        }

        return writer.ToString();
    }

    private static string? GetLiteralString(LogEvent e, PropertyToken property) =>
        property is { Format: "l", Alignment: null } &&
        e.Properties.TryGetValue(property.PropertyName, out var value) &&
        value is ScalarValue { Value: string literal } && value.GetType() == typeof(ScalarValue) ? literal : null;

    /// <summary>
    /// Writes event properties as a JSON object for a GCP log entry.
    /// </summary>
    public void WritePropertyAsJson(Struct jsonStruct, string propKey, LogEventPropertyValue propValue)
    {
        var value = CreatePropertyValue(propValue);
        if (value != null)
            jsonStruct.Fields.Add(propKey, value);
    }

    internal Value? CreatePropertyValue(LogEventPropertyValue propValue)
    {
        switch (propValue)
        {
            case ScalarValue scalar:
                return scalar.Value switch
                {
                    string text => Value.ForString(text),
                    null => Value.ForNull(),
                    bool boolean => Value.ForBool(boolean),
                    short or ushort or int or uint or long or ulong or float or double or decimal =>
                        Value.ForNumber(Convert.ToDouble(scalar.Value)),
                    _ => Value.ForString(scalar.Value.ToString() ?? "")
                };

            case SequenceValue sequenceValue:
                var sequenceChild = new ListValue();
                var elements = sequenceValue.Elements;
                var count = elements.Count;
                sequenceChild.Values.Capacity = count;
                for (var i = 0; i < count; i++)
                {
                    var element = CreatePropertyValue(elements[i]);
                    if (element != null)
                        sequenceChild.Values.Add(element);
                }

                return new Value { ListValue = sequenceChild };

            case StructureValue structureValue:
                var structureChild = new Struct();
                var properties = structureValue.Properties;
                for (var i = 0; i < properties.Count; i++)
                {
                    var childProperty = properties[i];
                    WritePropertyAsJson(structureChild, childProperty.Name, childProperty.Value);
                }

                return Value.ForStruct(structureChild);

            case DictionaryValue dictionaryValue:
                var dictionaryChild = new Struct();
                foreach (var childProperty in dictionaryValue.Elements)
                    WritePropertyAsJson(dictionaryChild, childProperty.Key.Value?.ToString() ?? "", childProperty.Value);

                return Value.ForStruct(dictionaryChild);
        }

        return null;
    }

    public static string? CreateLogName(string projectId, string name) =>
        EscapeLogId(name) is { } logId ? new LogName(projectId, logId).ToString() : null;

    internal static string? EscapeLogId(string name)
    {
#if NET8_0_OR_GREATER
        if (name.Length is > 0 and <= 500 && !name.AsSpan().ContainsAnyExcept(UnescapedLogIdChars))
            return name;
#endif

        // Allow only valid characters, then truncate below 512 characters and URL-encode.
        var safeChars = LogNameUnsafeChars().Replace(name, "");
        if (safeChars.Length == 0)
            return null;

        var truncated = safeChars.Length > 500 ? safeChars.Substring(0, 500) : safeChars;
        return UrlEncoder.Default.Encode(truncated);
    }
}