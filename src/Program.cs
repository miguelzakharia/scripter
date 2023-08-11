using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Data;
using System.Text;
using System.Text.Json;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Serilog;

var logger = new LoggerConfiguration()
	.WriteTo.Console()
	.CreateLogger();

var app = new CommandLineApplication();
app.HelpOption();
var configOption = app.Option("-c|--config <FILE>", "Path to configuration file", CommandOptionType.SingleValue)
	.IsRequired().Accepts(f => f.ExistingFile());
var outputOption = app.Option("-o|--output <FILE>", "Name of output file", CommandOptionType.SingleValue)
	.IsRequired().Accepts(f => f.LegalFilePath());

app.OnExecuteAsync(async cancellationToken =>
{
	using var reader = new StreamReader(configOption.Value());
	var config = JsonSerializer.Deserialize<ConfigOptions>(reader.BaseStream, new JsonSerializerOptions
	{
		PropertyNameCaseInsensitive = true
	});
	await Run(config.ServerConnectionString, config.DatabaseList.ToImmutableList(), outputOption.Value(), cancellationToken);
});

return app.Execute(args);

//------------------------
async Task Run(string connectionString, ImmutableList<string> databases, string outputPath, CancellationToken token)
{
	var sql = new StringBuilder();

	Console.SetIn(new StreamReader(Console.OpenStandardInput()));
	while (Console.In.Peek() != -1) sql.AppendLine(Console.In.ReadLine());

	var isValid = IsSQLQueryValid(sql.ToString(), out var parsingErrors);
	if (!isValid)
	{
		logger.Error("Query is not valid {Errors}", parsingErrors);
		return;
	}
	await File.WriteAllTextAsync(
		outputPath,
		await WriteHeaders(databases.First(), sql.ToString(), connectionString, token).ConfigureAwait(false),
		token);

	var resultBag = new ConcurrentBag<string>();

	await Parallel.ForEachAsync(
			databases,
			new ParallelOptions { MaxDegreeOfParallelism = 5 },
			async (database, token) =>
				resultBag.Add(await Execute(database, sql.ToString(), connectionString, token).ConfigureAwait(false))
		)
		.ConfigureAwait(false);

	await File.AppendAllLinesAsync(outputPath, resultBag, token);
}

async Task<string> Execute(string databaseName, string commandText, string connectionString, CancellationToken token)
{
	var builder = new StringBuilder();
	var rowCount = 0;

	try
	{
		var command = new SqlCommand
		{
			CommandText = commandText,
			CommandType = CommandType.Text
		};

		await using var conn = new SqlConnection(connectionString);
		await conn.OpenAsync(token).ConfigureAwait(false);
		await conn.ChangeDatabaseAsync(databaseName, token).ConfigureAwait(false);
		command.Connection = conn;
		await using (var results =
		             await command.ExecuteReaderAsync(CommandBehavior.CloseConnection, token).ConfigureAwait(false))
		{
			if (!results.HasRows) return string.Empty;
			rowCount = results.RecordsAffected;

			var fields = results.FieldCount;
			while (await results.ReadAsync(token).ConfigureAwait(false))
			{
				builder.Append($"{databaseName},");
				for (var x = 0; x < fields; x++)
				{
					var fieldType = results.GetFieldType(x);
					if (fieldType == typeof(string))
					{
						var value = results.GetFieldValue<string>(x).Contains(',')
							? $"\"{results.GetFieldValue<string>(x)}\""
							: results.GetFieldValue<string>(x);

						builder.Append(value);
					}
					else
					{
						builder.Append(results[x]);
					}

					if (x + 1 < fields) builder.Append(',');
				}

				builder.AppendLine();
			}
		}

		logger.Information("{DatabaseName} - {RowCount} rows affected", databaseName, rowCount);
	}
	catch (Exception e)
	{
		logger.Error(e, "Could not execute query for {DatabaseName}", databaseName);
		builder.Clear();
	}

	return builder.ToString();
}

async Task<string> WriteHeaders(
	string databaseName,
	string commandText,
	string connectionString,
	CancellationToken cancellationToken
)
{
	var builder = new StringBuilder();
	try
	{
		var command = new SqlCommand
		{
			CommandText = commandText,
			CommandType = CommandType.Text
		};
		await using var conn = new SqlConnection(connectionString);
		await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
		await conn.ChangeDatabaseAsync(databaseName, cancellationToken).ConfigureAwait(false);
		command.Connection = conn;

		await using (var results =
		             await command.ExecuteReaderAsync(CommandBehavior.CloseConnection, cancellationToken)
			             .ConfigureAwait(false))
		{
			if (!results.HasRows) return string.Empty;

			builder.Append("Database Name,");
			var fields = results.FieldCount;
			await results.ReadAsync(cancellationToken).ConfigureAwait(false);
			for (var x = 0; x < fields; x++)
			{
				builder.Append(results.GetName(x));
				if (x + 1 < fields) builder.Append(',');
			}

			builder.AppendLine();
		}
	}
	catch (Exception e)
	{
		logger.Error(e, "Cannot write headers");
		builder.Clear();
	}

	return builder.ToString();
}

bool IsSQLQueryValid(string sql, out List<string> errors)
{
    errors = new List<string>();
    var parser = new TSql160Parser(false);
    IList<ParseError> parseErrors;

    using TextReader reader = new StringReader(sql);
    var fragment = parser.Parse(reader, out parseErrors);
    if (parseErrors is { Count: > 0 })
    {
	    errors = parseErrors.Select(e => e.Message).ToList();
	    return false;
    }

    return true;
}

public record ConfigOptions(string ServerConnectionString, string[] DatabaseList, int MaxParallelism = 5);
