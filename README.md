# scripter

## Building the project

```csharp
dotnet build
```

## Sample use

From folder that contains the source:

```csharp
cat my-sql-script.sql | /path/to/scripter -c config-file.json -o /path/to/output.csv
```

See `src/config.template.json` for a sample config file.
