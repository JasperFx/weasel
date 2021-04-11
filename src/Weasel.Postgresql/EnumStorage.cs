namespace Weasel.Postgresql
{
    /// <summary>
    /// Governs how .Net Enum types are persisted
    /// in the serialized JSON
    /// </summary>
    public enum EnumStorage
    {
        /// <summary>
        /// Serialize Enum values as their integer value
        /// </summary>
        AsInteger,

        /// <summary>
        /// Serialize Enum values as their string value
        /// </summary>
        AsString
    }
}