using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Encodings.Web;
using System.Text.RegularExpressions;
using Google.Cloud.Logging.V2;
using Google.Protobuf.WellKnownTypes;
using Serilog.Events;
using Serilog.Formatting.Display;

namespace Serilog.Sinks.GoogleCloudLogging
{
    internal class LogFormatter
    {
        private readonly string _projectId;
        private readonly bool _useSourceContextAsLogName;
        private readonly MessageTemplateTextFormatter _messageTemplateTextFormatter;

        private static readonly Regex LogNameUnsafeChars = new Regex("[^0-9A-Z._/-]+", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.CultureInvariant);

        public LogFormatter(string projectId, bool useSourceContextAsLogName, MessageTemplateTextFormatter messageTemplateTextFormatter)
        {
            _projectId = projectId;
            _useSourceContextAsLogName = useSourceContextAsLogName;
            _messageTemplateTextFormatter = messageTemplateTextFormatter;
        }

        public string RenderEventMessage(LogEvent e, StringWriter writer)
        {
            // output template takes priority for formatting event
            if (_messageTemplateTextFormatter != null)
            {
                writer.GetStringBuilder().Clear();
                _messageTemplateTextFormatter.Format(e, writer);
                return writer.ToString();
            }

            // otherwise manually format message and handle exceptions
            string msg = e.RenderMessage();
            string exceptionMessage = null; 
            if (e.Exception != null)
            {
                //if (e.Exception.GetType() == typeof(AggregateException))
                //{
                //    // ErrorReporting won't report all InnerExceptions for an AggregateException. This work-around isn't perfect but better than the default behavior
                //    lines.AddRange(((AggregateException) e.Exception).Flatten().InnerExceptions.Select(s => s.Message));
                //}

                exceptionMessage = e.Exception.ToString();
            }

            bool hasMsg = !String.IsNullOrWhiteSpace(msg);
            bool hasExc = !String.IsNullOrWhiteSpace(exceptionMessage);
            if (hasMsg && hasExc)
            {
                return msg + "\n" + exceptionMessage;
            }
            if (hasMsg)
            {
                return msg;
            }

            if(hasExc)
            {
                return exceptionMessage;
            }

            return String.Empty;
        }

        /// <summary>
        /// Writes event properties as a JSON object for a GCP log entry.
        /// </summary>
        public void WritePropertyAsJson(LogEntry log, Struct jsonStruct, string propKey, LogEventPropertyValue propValue)
        {
            switch (propValue)
            {
                case ScalarValue scalarValue when scalarValue.Value is null:
                    jsonStruct.Fields.Add(propKey, Value.ForNull());
                    break;

                case ScalarValue scalarValue when scalarValue.Value is bool boolValue:
                    jsonStruct.Fields.Add(propKey, Value.ForBool(boolValue));
                    break;

                case ScalarValue scalarValue
                    when scalarValue.Value is short || scalarValue.Value is ushort || scalarValue.Value is int
                         || scalarValue.Value is uint || scalarValue.Value is long || scalarValue.Value is ulong
                         || scalarValue.Value is float || scalarValue.Value is double || scalarValue.Value is decimal:

                    // all numbers are converted to double and may lose precision
                    // numbers should be sent as strings if they do not fit in a double
                    jsonStruct.Fields.Add(propKey, Value.ForNumber(Convert.ToDouble(scalarValue.Value)));
                    break;

                case ScalarValue scalarValue when scalarValue.Value is string stringValue:
                    jsonStruct.Fields.Add(propKey, Value.ForString(stringValue));
                    CheckIfSourceContext(log, propKey, stringValue);
                    break;

                case ScalarValue scalarValue:
                    // handle all other scalar values as strings
                    var strValue = scalarValue.Value.ToString();
                    jsonStruct.Fields.Add(propKey, Value.ForString(strValue));
                    CheckIfSourceContext(log, propKey, strValue);
                    break;

                case SequenceValue sequenceValue:
                    var sequenceChild = new Struct();
                    for (var i = 0; i < sequenceValue.Elements.Count; i++)
                        WritePropertyAsJson(log, sequenceChild, i.ToString(), sequenceValue.Elements[i]);

                    jsonStruct.Fields.Add(propKey, Value.ForList(sequenceChild.Fields.Values.ToArray()));
                    break;

                case StructureValue structureValue:
                    var structureChild = new Struct();
                    foreach (var childProperty in structureValue.Properties)
                        WritePropertyAsJson(log, structureChild, childProperty.Name, childProperty.Value);

                    jsonStruct.Fields.Add(propKey, Value.ForStruct(structureChild));
                    break;

                case DictionaryValue dictionaryValue:
                    var dictionaryChild = new Struct();
                    foreach (var childProperty in dictionaryValue.Elements)
                        WritePropertyAsJson(log, dictionaryChild, childProperty.Key.Value?.ToString(), childProperty.Value);

                    jsonStruct.Fields.Add(propKey, Value.ForStruct(dictionaryChild));
                    break;
            }
        }

        /// <summary>
        /// Writes event properties as labels for a GCP log entry.
        /// GCP log labels are a flat key/value namespace so all child event properties will be prefixed with parent property names "parentkey.childkey" similar to json path.
        /// </summary>
        public void WritePropertyAsLabel(LogEntry log, string propertyKey, LogEventPropertyValue propertyValue)
        {
            switch (propertyValue)
            {
                case ScalarValue scalarValue when scalarValue.Value is null:
                    log.Labels.Add(propertyKey, String.Empty);
                    break;

                case ScalarValue scalarValue:
                    var stringValue = scalarValue.Value.ToString();
                    log.Labels.Add(propertyKey, stringValue);
                    CheckIfSourceContext(log, propertyKey, stringValue);
                    break;

                case SequenceValue sequenceValue:
                    log.Labels.Add(propertyKey, String.Join(",", sequenceValue.Elements));
                    break;

                case StructureValue structureValue when structureValue.Properties.Count > 0:
                    foreach (var childProperty in structureValue.Properties)
                        WritePropertyAsLabel(log, propertyKey + "." + childProperty.Name, childProperty.Value);

                    break;

                case DictionaryValue dictionaryValue when dictionaryValue.Elements.Count > 0:
                    foreach (var childProperty in dictionaryValue.Elements)
                        WritePropertyAsLabel(log, propertyKey + "." + childProperty.Key.ToString().Replace("\"", ""), childProperty.Value);

                    break;
            }
        }

        private void CheckIfSourceContext(LogEntry log, string propertyKey, string stringValue)
        {
            if (_useSourceContextAsLogName && propertyKey.Equals("SourceContext", StringComparison.OrdinalIgnoreCase))
                log.LogName = CreateLogName(_projectId, stringValue);
        }

        public static string CreateLogName(string projectId, string name)
        {
            // name must only contain letters, numbers, underscore, hyphen, forward slash and period
            // limited to 512 characters and must be url-encoded
            var safeChars = LogNameUnsafeChars.Replace(name, String.Empty);
            var clean = UrlEncoder.Default.Encode(safeChars);
            
            // LogName class creates templated string matching GCP requirements
            return new LogName(projectId, clean).ToString();
        }
    }
}
