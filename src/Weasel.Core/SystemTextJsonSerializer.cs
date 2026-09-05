using System.Buffers;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace Weasel.Core;

/// <summary>
///     Default serializer implementation using System.Text.Json, shared by the Critter Stack
///     document stores (Polecat, Fisher). Lifted from their byte-identical per-store copies
///     (weasel#555); each store keeps a small subclass in its own namespace for back-compat.
/// </summary>
/// <remarks>
///     Also carries every member of <c>Weasel.Storage.IStorageSerializer</c> — the dialect-neutral
///     serializer seam of the shared closed-shape storage runtime (weasel#329/#331,
///     JasperFx/polecat#273) — with BCL-typed signatures only, so a store's subclass can declare
///     that interface and satisfy it entirely through these inherited members. The interface itself
///     lives in Weasel.Storage, which references Weasel.Core, so it cannot be declared here.
/// </remarks>
public class SystemTextJsonSerializer : ISerializer
{
    private JsonSerializerOptions _options;
    private EnumStorage _enumStorage = EnumStorage.AsInteger;
    private Casing _casing = Casing.CamelCase;
    private CollectionStorage _collectionStorage = CollectionStorage.Default;
    private NonPublicMembersStorage _nonPublicMembersStorage = NonPublicMembersStorage.Default;

    public SystemTextJsonSerializer() : this(DefaultOptions())
    {
    }

    public SystemTextJsonSerializer(JsonSerializerOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    ///     Provides access to the underlying JsonSerializerOptions for advanced configuration.
    /// </summary>
    public JsonSerializerOptions Options => _options;

    public EnumStorage EnumStorage
    {
        get => _enumStorage;
        set
        {
            _enumStorage = value;
            ApplyEnumStorage();
        }
    }

    public Casing Casing
    {
        get => _casing;
        set
        {
            _casing = value;
            ApplyCasing();
        }
    }

    public CollectionStorage CollectionStorage
    {
        get => _collectionStorage;
        set => _collectionStorage = value;
    }

    public NonPublicMembersStorage NonPublicMembersStorage
    {
        get => _nonPublicMembersStorage;
        set
        {
            _nonPublicMembersStorage = value;
            ApplyNonPublicMembers();
        }
    }

    /// <summary>
    ///     Apply custom configuration to the underlying JsonSerializerOptions.
    /// </summary>
    public void Configure(Action<JsonSerializerOptions> configure)
    {
        configure(_options);
    }

    // polecat#273: the shared members inherited from Weasel.Core.ISerializer are intentionally
    // unannotated (Marten parity), so their reflection-based STJ bodies suppress the RUC/RDC warnings
    // here rather than propagating a caller-facing contract the base doesn't declare. AOT consumers
    // supply a source-generator-backed ISerializer. The two string-based overloads below are
    // store-level extensions (Polecat and Fisher declare them on their own ISerializer interfaces,
    // not on the shared base), so they keep the propagating
    // [RequiresUnreferencedCode]/[RequiresDynamicCode].
    private const string SharedSerializerSuppression =
        "Reflection-based STJ serialize/deserialize is the Critter Stack stores' documented default; the shared Weasel.Core.ISerializer base is intentionally unannotated for Marten parity, so RUC/RDC is not propagated. AOT consumers supply a source-generator-backed ISerializer.";

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode", Justification = SharedSerializerSuppression)]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = SharedSerializerSuppression)]
    public string ToJson(object? document)
    {
        // object? satisfies both Weasel.Core.ISerializer.ToJson(object) and the wider
        // Weasel.Storage.IStorageSerializer.ToJson(object?) with a single implementation.
        return JsonSerializer.Serialize(document, document?.GetType() ?? typeof(object), _options);
    }

    // ---- Weasel.Storage.IStorageSerializer additions (beyond the Weasel.Core.ISerializer base) ----

    /// <summary>
    ///     STJ has no type-metadata pollution to strip, so "clean" JSON is identical to
    ///     <see cref="ToJson" />. (The distinction exists on the shared seam for Newtonsoft's
    ///     TypeNameHandling.)
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode", Justification = SharedSerializerSuppression)]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = SharedSerializerSuppression)]
    public string ToCleanJson(object? document)
    {
        return ToJson(document);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode", Justification = SharedSerializerSuppression)]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = SharedSerializerSuppression)]
    public void WriteTo(IBufferWriter<byte> writer, object? value)
    {
        using var jsonWriter = new Utf8JsonWriter(writer);
        JsonSerializer.Serialize(jsonWriter, value, value?.GetType() ?? typeof(object), _options);
    }

    /// <summary>
    ///     Writes the value's JSON to a database parameter as a plain string value. Every store
    ///     using this serializer binds JSON as string parameter values throughout (SQL Server 2025's
    ///     native JSON column type accepts nvarchar input; SQLite's json1 functions accept TEXT), so
    ///     this stays dialect-neutral with no provider-specific typing.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode", Justification = SharedSerializerSuppression)]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = SharedSerializerSuppression)]
    public void WriteToParameter(DbParameter parameter, object? value)
    {
        parameter.Value = value is null ? DBNull.Value : ToJson(value);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode", Justification = SharedSerializerSuppression)]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = SharedSerializerSuppression)]
    public async ValueTask<T> FromJsonAsync<T>(DbDataReader reader, int index,
        CancellationToken cancellationToken = default)
    {
        var json = await reader.GetFieldValueAsync<string>(index, cancellationToken).ConfigureAwait(false);
        return FromJson<T>(json);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode", Justification = SharedSerializerSuppression)]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = SharedSerializerSuppression)]
    public async ValueTask<object> FromJsonAsync(Type type, DbDataReader reader, int index,
        CancellationToken cancellationToken = default)
    {
        var json = await reader.GetFieldValueAsync<string>(index, cancellationToken).ConfigureAwait(false);
        return FromJson(type, json);
    }

    [RequiresUnreferencedCode("STJ JsonSerializer.Deserialize<T> uses reflection over T.")]
    [RequiresDynamicCode("STJ JsonSerializer.Deserialize requires runtime code generation for non-source-generated types.")]
    public T FromJson<T>(string json)
    {
        return JsonSerializer.Deserialize<T>(json, _options)!;
    }

    [RequiresUnreferencedCode("STJ JsonSerializer.Deserialize uses reflection over the supplied type.")]
    [RequiresDynamicCode("STJ JsonSerializer.Deserialize requires runtime code generation for non-source-generated types.")]
    public object FromJson(Type type, string json)
    {
        return JsonSerializer.Deserialize(json, type, _options)!;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode", Justification = SharedSerializerSuppression)]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = SharedSerializerSuppression)]
    public T FromJson<T>(Stream stream)
    {
        return JsonSerializer.Deserialize<T>(stream, _options)!;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode", Justification = SharedSerializerSuppression)]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = SharedSerializerSuppression)]
    public object FromJson(Type type, Stream stream)
    {
        return JsonSerializer.Deserialize(stream, type, _options)!;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode", Justification = SharedSerializerSuppression)]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = SharedSerializerSuppression)]
    public T FromJson<T>(DbDataReader reader, int index)
    {
        var json = reader.GetString(index);
        return FromJson<T>(json);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode", Justification = SharedSerializerSuppression)]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = SharedSerializerSuppression)]
    public object FromJson(Type type, DbDataReader reader, int index)
    {
        var json = reader.GetString(index);
        return FromJson(type, json);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode", Justification = SharedSerializerSuppression)]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = SharedSerializerSuppression)]
    public async ValueTask<T> FromJsonAsync<T>(Stream stream, CancellationToken cancellationToken = default)
    {
        return (await JsonSerializer.DeserializeAsync<T>(stream, _options, cancellationToken).ConfigureAwait(false))!;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode", Justification = SharedSerializerSuppression)]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = SharedSerializerSuppression)]
    public async ValueTask<object> FromJsonAsync(Type type, Stream stream,
        CancellationToken cancellationToken = default)
    {
        return (await JsonSerializer.DeserializeAsync(stream, type, _options, cancellationToken).ConfigureAwait(false))!;
    }

    public static JsonSerializerOptions DefaultOptions()
    {
        return new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = false
        };
    }

    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "JsonStringEnumConverter(JsonNamingPolicy, bool) requires runtime code generation for non-source-generated enum types. The default serializer is reflection-based by design; AOT consumers should supply a custom ISerializer with the generic JsonStringEnumConverter<TEnum> per-enum.")]
    private void ApplyEnumStorage()
    {
        // Remove any existing JsonStringEnumConverter
        for (var i = _options.Converters.Count - 1; i >= 0; i--)
        {
            if (_options.Converters[i] is JsonStringEnumConverter)
            {
                _options.Converters.RemoveAt(i);
            }
        }

        if (_enumStorage == EnumStorage.AsString)
        {
            _options.Converters.Add(new JsonStringEnumConverter(_options.PropertyNamingPolicy));
        }
    }

    private void ApplyCasing()
    {
        _options.PropertyNamingPolicy = _casing switch
        {
            Casing.CamelCase => JsonNamingPolicy.CamelCase,
            Casing.SnakeCase => JsonNamingPolicy.SnakeCaseLower,
            Casing.Default => null,
            _ => null
        };
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "DefaultJsonTypeInfoResolver uses reflection. The whole NonPublicMembers feature is reflection-driven by intent; AOT consumers should supply a custom ISerializer with an STJ source-generator context.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Same as IL2026.")]
    [UnconditionalSuppressMessage("Trimming", "IL2075:DynamicallyAccessedMembers",
        Justification = "Falls back to scanning typeInfo.Type.GetProperties when the JSON property has no usable AttributeProvider. Reflection on user types — keeping public/non-public properties alive is the user's responsibility when opting into NonPublicMembers.")]
    private void ApplyNonPublicMembers()
    {
        if (_nonPublicMembersStorage == NonPublicMembersStorage.Default)
        {
            return;
        }

        var resolver = new DefaultJsonTypeInfoResolver();

        if (_nonPublicMembersStorage.HasFlag(NonPublicMembersStorage.NonPublicSetters))
        {
            resolver.Modifiers.Add(typeInfo =>
            {
                if (typeInfo.Kind != JsonTypeInfoKind.Object) return;

                foreach (var property in typeInfo.Properties)
                {
                    if (property.Set == null)
                    {
                        // property.Name is the JSON name (e.g. "name" in camelCase), so look up
                        // by AttributeProvider which gives us the actual PropertyInfo
                        var clrProperty = property.AttributeProvider as System.Reflection.PropertyInfo;
                        if (clrProperty == null)
                        {
                            // Fallback: search all properties by case-insensitive match
                            clrProperty = typeInfo.Type.GetProperties(
                                    System.Reflection.BindingFlags.Public |
                                    System.Reflection.BindingFlags.NonPublic |
                                    System.Reflection.BindingFlags.Instance)
                                .FirstOrDefault(p => string.Equals(p.Name, property.Name,
                                    StringComparison.OrdinalIgnoreCase));
                        }

                        if (clrProperty?.GetSetMethod(true) != null)
                        {
                            property.Set = (obj, value) => clrProperty.GetSetMethod(true)!.Invoke(obj, [value]);
                        }
                    }
                }
            });
        }

        _options.TypeInfoResolver = resolver;

        if (_nonPublicMembersStorage.HasFlag(NonPublicMembersStorage.NonPublicDefaultConstructor) ||
            _nonPublicMembersStorage.HasFlag(NonPublicMembersStorage.NonPublicConstructor))
        {
            _options.PreferredObjectCreationHandling = JsonObjectCreationHandling.Populate;
        }
    }
}
