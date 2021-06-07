using System;
using Baseline.ImTools;

namespace Weasel.Core
{
    public abstract class TypeMappingsBase<TParameterType> where TParameterType : struct
    {
        protected readonly Ref<ImHashMap<Type, string>> DatabaseTypeMemo = Ref.Of(ImHashMap<Type, string>.Empty);
        protected readonly Ref<ImHashMap<Type, TParameterType?>> ParameterTypeMemo = Ref.Of(ImHashMap<Type, TParameterType?>.Empty);
        protected readonly Ref<ImHashMap<TParameterType, Type[]>> TypeMemo = Ref.Of(ImHashMap<TParameterType, Type[]>.Empty);

        protected abstract bool determineNpgsqlDbType(Type type, out TParameterType dbType);
        
        public TParameterType? TryGetDbType(Type? type)
        {
            if (type == null || !determineNpgsqlDbType(type, out var dbType))
                return null;

            return dbType;
        }
        
        public TParameterType ToDbType(Type type)
        {
            if (determineNpgsqlDbType(type, out var dbType))
                return dbType;

            throw new NotSupportedException($"Can't infer {typeof(TParameterType).Name} for type " + type);
        }
        
        public void RegisterMapping(Type type, string databaseType, TParameterType? parameterType)
        {
            DatabaseTypeMemo.Swap(d => d.AddOrUpdate(type, databaseType));
            ParameterTypeMemo.Swap(d => d.AddOrUpdate(type, parameterType));
        }

        protected abstract Type[] determineClrTypesForParameterType(TParameterType dbType);

        public Type[] ResolveTypes(TParameterType parameterType)
        {
            if (TypeMemo.Value.TryFind(parameterType, out var values))
                return values;

            values = determineClrTypesForParameterType(parameterType);

            TypeMemo.Swap(d => d.AddOrUpdate(parameterType, values!));

            return values!;
        }

        public abstract string GetDatabaseType(Type memberType, EnumStorage enumStyle);
    }
}