using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace Weasel.EntityFrameworkCore.Batching;

/// <summary>
///     Materializes entities from a <see cref="DbDataReader" /> using EF Core's
///     <see cref="IEntityType" /> metadata for column-to-property mapping and value conversion.
/// </summary>
internal static class EntityMaterializer
{
    /// <summary>
    ///     Builds a mapping from reader ordinals to entity properties for the given entity type.
    /// </summary>
    public static PropertyColumnMapping[] GetColumnMap<T>(IEntityType entityType, DbDataReader reader)
    {
        var properties = entityType.GetProperties();
        var mappings = new List<PropertyColumnMapping>();

        foreach (var property in properties)
        {
            var columnName = property.GetColumnName();
            if (columnName == null) continue;

            var ordinal = -1;
            try
            {
                ordinal = reader.GetOrdinal(columnName);
            }
            catch (IndexOutOfRangeException)
            {
                // Column not in result set — skip (could be a shadow property not selected)
                continue;
            }

            mappings.Add(new PropertyColumnMapping
            {
                Property = property,
                Ordinal = ordinal,
                ClrType = property.ClrType,
                ValueConverter = property.GetValueConverter(),
                IsNullable = property.IsNullable
            });
        }

        return mappings.ToArray();
    }

    /// <summary>
    ///     Creates and populates a new entity instance from the current row.
    /// </summary>
    public static T Materialize<T>(DbDataReader reader, PropertyColumnMapping[] mappings) where T : class, new()
    {
        var entity = new T();

        foreach (var mapping in mappings)
        {
            if (reader.IsDBNull(mapping.Ordinal))
            {
                if (mapping.IsNullable)
                {
                    mapping.Property.PropertyInfo?.SetValue(entity, null);
                }
                continue;
            }

            var rawValue = reader.GetValue(mapping.Ordinal);

            if (mapping.ValueConverter != null)
            {
                rawValue = mapping.ValueConverter.ConvertFromProvider(rawValue)!;
            }
            else if (rawValue.GetType() != mapping.ClrType)
            {
                rawValue = ConvertValue(rawValue, mapping.ClrType);
            }

            mapping.Property.PropertyInfo?.SetValue(entity, rawValue);
        }

        return entity;
    }

    private static object ConvertValue(object value, Type targetType)
    {
        var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;

        if (underlying.IsEnum)
        {
            return Enum.ToObject(underlying, value);
        }

        if (underlying == typeof(Guid) && value is string s)
        {
            return Guid.Parse(s);
        }

        return Convert.ChangeType(value, underlying);
    }
}

internal struct PropertyColumnMapping
{
    public IProperty Property;
    public int Ordinal;
    public Type ClrType;
    public ValueConverter? ValueConverter;
    public bool IsNullable;
}
