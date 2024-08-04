using SpaceDb.Core.Utils;

namespace SpaceDb.Core.Indexes;

public class SpatialIndex
{
    private readonly string _filePath;
    private Dictionary<Tuple<double, double>, List<long>> _offsets;
    private readonly object _lock = new();

    public SpatialIndex(string filePath)
    {
        _filePath = filePath;
        _offsets = new Dictionary<Tuple<double, double>, List<long>>();
    }

    internal void Add(double latitude, double longitude, long offset)
    {
        lock (_lock)
        {
            if (!_offsets.Any())
            {
                LoadIndex();
            }

            var key = Tuple.Create(latitude, longitude);
            if (_offsets.TryGetValue(new Tuple<double, double>(latitude, longitude), out var result))
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

            WriteToFile(_offsets);
        }
    }

    internal List<long> Offsets(double latitude, double longitude, double rangeInMeters, int limit)
    {
        lock (_lock)
        {
            if (!_offsets.Any())
            {
                LoadIndex();
            }

            return _offsets
                .Where(kvp => GeoUtils.HaversineDistance(
                    latitude, longitude,
                    kvp.Key.Item1, kvp.Key.Item2) <= rangeInMeters)
                .Take(limit)
                .SelectMany(e => e.Value)
                .ToList();
        }
    }
    
    private void LoadIndex()
    {
        _offsets = ReadFromFile();
    }

    private void WriteToFile(Dictionary<Tuple<double, double>, List<long>> dictionary)
    {
        using var fileStream = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.Write);
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
    private Dictionary<Tuple<double, double>, List<long>> ReadFromFile()
    {
        var dictionary = new Dictionary<Tuple<double, double>, List<long>>();

        using var fileStream = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.Read);
        using var reader = new BinaryReader(fileStream);

        if (reader.BaseStream.Length == 0)
        {
            return new();
        }

        var count = reader.ReadInt32();

        for (var i = 0; i < count; i++)
        {
            var item1 = reader.ReadDouble();
            var item2 = reader.ReadDouble();
            var value = reader.ReadInt64();
            var key = Tuple.Create(item1, item2);

            if (dictionary.TryGetValue(key, out var value1))
            {
                value1.Add(value);
            }
            else
            {
                dictionary.Add(Tuple.Create(item1, item2), new List<long>()
                {
                    value
                });
            }
        }

        return dictionary;
    }
}