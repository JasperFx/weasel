using System;
using System.Data.Common;
using Baseline.ImTools;

namespace Weasel.Core
{
    public interface IDatabaseProvider<TCommand, TParameter, TConnection, TTransaction, TParameterType, TDataReader>
        where TCommand : DbCommand, new()
        where TParameter : DbParameter
        where TConnection : DbConnection
        where TTransaction : DbTransaction
        where TDataReader : DbDataReader
        where TParameterType : struct
    {
        TParameterType? TryGetDbType(Type? type);
        TParameterType ToParameterType(Type type);
        Type[] ResolveTypes(TParameterType parameterType);
        string GetDatabaseType(Type memberType, EnumStorage enumStyle);


        void AddParameter(TCommand command, TParameter parameter);
        void SetParameterType(TParameter parameter, TParameterType dbType);
        
        TParameterType StringParameterType { get; }
        TParameterType IntegerParameterType { get; }
        TParameterType LongParameterType { get; }
        TParameterType GuidParameterType { get; }
        TParameterType BoolParameterType { get; }
        TParameterType DoubleParameterType { get; }
    }

    public abstract class DatabaseProvider<TCommand, TParameter, TConnection, TTransaction, TParameterType, TDataReader>
        : IDatabaseProvider<TCommand, TParameter, TConnection, TTransaction, TParameterType, TDataReader>
        where TCommand : DbCommand, new()
        where TParameter : DbParameter
        where TConnection : DbConnection
        where TTransaction : DbTransaction
        where TDataReader : DbDataReader
        where TParameterType : struct  
    {
        protected readonly Ref<ImHashMap<Type, string>> DatabaseTypeMemo = Ref.Of(ImHashMap<Type, string>.Empty);
        protected readonly Ref<ImHashMap<Type, TParameterType?>> ParameterTypeMemo = Ref.Of(ImHashMap<Type, TParameterType?>.Empty);
        protected readonly Ref<ImHashMap<TParameterType, Type[]>> TypeMemo = Ref.Of(ImHashMap<TParameterType, Type[]>.Empty);

        protected DatabaseProvider()
        {
            StringParameterType = ToParameterType(typeof(string));
            IntegerParameterType = ToParameterType(typeof(int));
            LongParameterType = ToParameterType(typeof(long));
            GuidParameterType = ToParameterType(typeof(Guid));
            BoolParameterType = ToParameterType(typeof(bool));
            DoubleParameterType = ToParameterType(typeof(double));
        }

        protected abstract bool determineNpgsqlDbType(Type type, out TParameterType dbType);
        
        public TParameterType? TryGetDbType(Type? type)
        {
            if (type == null || !determineNpgsqlDbType(type, out var dbType))
                return null;

            return dbType;
        }
        
        public TParameterType ToParameterType(Type type)
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


        public abstract void AddParameter(TCommand command, TParameter parameter);

        public abstract void SetParameterType(TParameter parameter, TParameterType dbType);


        public TParameterType StringParameterType { get; }
        public TParameterType IntegerParameterType { get; }
        public TParameterType LongParameterType { get; }
        public TParameterType GuidParameterType { get; }
        public TParameterType BoolParameterType { get; }
        public TParameterType DoubleParameterType { get; }
    }
}