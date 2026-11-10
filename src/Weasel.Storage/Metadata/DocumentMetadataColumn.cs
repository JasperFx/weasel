#nullable enable
using System;
using System.Reflection;

namespace Weasel.Storage.Metadata;

/// <summary>
/// weasel#745: the configuration of one document metadata column, shared by the stores that build
/// their closed-shape storage from Weasel.Storage. Says whether the column is in the table
/// (<see cref="Enabled"/>), what it is called (<see cref="Name"/>), and which document member, if
/// any, the stored value is projected onto when a document is loaded (<see cref="Member"/>).
/// </summary>
/// <remarks>
/// <para>
/// Named <c>DocumentMetadataColumn</c> rather than <c>MetadataColumn</c>, and kept in its own
/// namespace, because Marten, Polecat and Fisher each have a <c>MetadataColumn</c> of their own,
/// and their descriptor builders import <c>Weasel.Storage</c> beside their own metadata namespace:
/// a second <c>MetadataColumn</c> in <c>Weasel.Storage</c> would be an ambiguous-reference error
/// (CS0104) in all of those files.
/// </para>
/// <para>
/// Whether a column is read back at all is decided by <see cref="Member"/>: a binder whose column
/// maps onto no member has nothing to write the value to, so
/// <see cref="DocumentBinderPlan{TDoc}"/> leaves it out of the SELECT.
/// </para>
/// </remarks>
public sealed class DocumentMetadataColumn
{
    private string _name;

    public DocumentMetadataColumn(string name)
    {
        _name = checkName(name);
    }

    /// <summary>
    /// The SQL column name. Settable because the stores do not agree on names (Marten's
    /// <c>mt_version</c>, Polecat's and Fisher's <c>guid_version</c>); the owning store sets its own
    /// when it creates the column set.
    /// </summary>
    public string Name
    {
        get => _name;
        set => _name = checkName(value);
    }

    /// <summary>
    /// Whether the column is persisted for the document type.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// The document member the stored value is mapped to and from, or <c>null</c> when the value is
    /// stored but not projected onto the document.
    /// </summary>
    public MemberInfo? Member { get; set; }

    /// <summary>
    /// True when the column is enabled and mapped onto a member — the condition under which a
    /// binder for it belongs in the read set.
    /// </summary>
    public bool EnabledWithMember => Enabled && Member is not null;

    /// <summary>
    /// Copy another column's configuration over this one where the other says something: enable
    /// this column if the other is enabled, and take its member mapping if it has one. Never
    /// disables, never clears a mapping, and never changes <see cref="Name"/>.
    /// </summary>
    public void MergeFrom(DocumentMetadataColumn other)
    {
        ArgumentNullException.ThrowIfNull(other);

        if (other.Enabled)
        {
            Enabled = true;
        }

        if (other.Member is not null)
        {
            Member = other.Member;
        }
    }

    public override string ToString() => $"{Name} (Enabled: {Enabled}, Member: {Member?.Name ?? "none"})";

    private static string checkName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("A metadata column needs a name.", nameof(name));
        }

        return name;
    }
}
