using Newtonsoft.Json.Linq;

namespace Weasel.Core.Operations.DirtyTracking;

public class ChangeTracker<T>: IChangeTracker
{
    private readonly T _document;
    private string _json;

    public ChangeTracker(IStorageSession session, T document)
    {
        _document = document;
        _json = session.Serializer.ToCleanJson(document);
    }

    public object Document => _document;

    public bool DetectChanges(IStorageSession session, out IStorageOperation operation)
    {
        var newJson = session.Serializer.ToCleanJson(_document);
        if (JToken.DeepEquals(JObject.Parse(_json), JObject.Parse(newJson)))
        {
            operation = null;
            return false;
        }

        operation = session.UpsertDirtyCheckedDocument(_document);

        return true;
    }

    public void Reset(IStorageSession session)
    {
        _json = session.Serializer.ToCleanJson(_document);
    }
}
