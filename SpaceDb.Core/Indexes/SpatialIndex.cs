using SpaceDb.Core.Utils;

namespace SpaceDb.Core.Indexes;

public class SpatialIndex
{
    private readonly string _filePath = "spatialIndex_{0}.db";
    private Dictionary<Tuple<string, double, double>, List<long>> _offsets;
    private readonly object _lock = new();

    public SpatialIndex()
    {
        _offsets = new Dictionary<Tuple<string, double, double>, List<long>>();
    }

    internal void Add<T>(double latitude, double longitude, long offset) where T : BaseEntity
    {
        lock (_lock)
        {
            if (!_offsets.Any())
            {
                LoadIndex<T>();
            }

            var typeName = typeof(T).Name;

            var key = Tuple.Create(typeName, latitude, longitude);
            if (_offsets.TryGetValue(key, out var result))
            {
                _offsets[key].Add(offset);
            }
            else
            {
                _offsets.Add(key, new List<long>()
                {
                    offset
                });
            }

            WriteToFile<T>(_offsets);
        }
    }

    internal List<long> Offsets<T>(double latitude, double longitude, double rangeInMeters, int limit) where T : BaseEntity
    {
        lock (_lock)
        {
            if (!_offsets.Any())
            {
                LoadIndex<T>();
            }

            var typeName = typeof(T).Name;

            return _offsets
                .Where(kvp => kvp.Key.Item1 == typeName && GeoUtils.HaversineDistance(
                    latitude, longitude,
                    kvp.Key.Item2, kvp.Key.Item3) <= rangeInMeters)
                .Take(limit)
                .SelectMany(e => e.Value)
                .ToList();
        }
    }
    
    private void LoadIndex<T>() where T : BaseEntity
    {
        var typeName = typeof(T).Name;
        var dictionary = new Dictionary<Tuple<string, double, double>, List<long>>();

        using var fileStream = new FileStream(string.Format(_filePath, typeName), FileMode.OpenOrCreate, FileAccess.Read);
        using var reader = new BinaryReader(fileStream);

        if (reader.BaseStream.Length == 0)
        {
            return;
        }

        var count = reader.ReadInt32();

        for (var i = 0; i < count; i++)
        {
            var item1 = reader.ReadDouble();
            var item2 = reader.ReadDouble();
            var value = reader.ReadInt64();
            var key = Tuple.Create(typeName, item1, item2);

            if (_offsets.TryGetValue(key, out var value1))
            {
                value1.Add(value);
            }
            else
            {
                _offsets.Add(key, new List<long>()
                {
                    value
                });
            }
        }
    }

    private void WriteToFile<T>(Dictionary<Tuple<string, double, double>, List<long>> dictionary) where T : BaseEntity
    {
        var typeName = typeof(T).Name;
        using var fileStream = new FileStream(string.Format(_filePath, typeName), FileMode.OpenOrCreate, FileAccess.Write);
        using var writer = new BinaryWriter(fileStream);
        writer.Write(dictionary.Count);

        foreach (var kvp in dictionary)
        {
            foreach (var value in kvp.Value)
            {
                writer.Write(kvp.Key.Item1);
                writer.Write(kvp.Key.Item2);
                writer.Write(value);
            }
        }
    }
}