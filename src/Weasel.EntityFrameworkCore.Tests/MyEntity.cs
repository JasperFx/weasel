using Weasel.Core;

namespace Weasel.EntityFrameworkCore.Tests;

public class MyEntity
{
    public Guid Id { get; set; }

    // Non-nullable value types
    public int IntValue { get; set; }
    public bool BoolValue { get; set; }
    public string StringValue { get; set; } = string.Empty;
    public Guid GuidValue { get; set; }
    public DateOnly DateOnlyValue { get; set; }
    public TimeOnly TimeOnlyValue { get; set; }
    public DateTime DateTimeValue { get; set; }
    public DateTimeOffset DateTimeOffsetValue { get; set; }
    public CascadeAction CascadeActionValue { get; set; }

    // Nullable value types
    public int? NullableIntValue { get; set; }
    public bool? NullableBoolValue { get; set; }
    public Guid? NullableGuidValue { get; set; }
    public DateOnly? NullableDateOnlyValue { get; set; }
    public TimeOnly? NullableTimeOnlyValue { get; set; }
    public DateTime? NullableDateTimeValue { get; set; }
    public DateTimeOffset? NullableDateTimeOffsetValue { get; set; }
    public CascadeAction? NullableCascadeActionValue { get; set; }
}
