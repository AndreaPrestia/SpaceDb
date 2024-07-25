using SpaceDb.Core.Utils;

namespace SpaceDb.Core.Indexes;

public class SpatialIndex
{
    private readonly string _filePath;
    private Dictionary<Tuple<double, double>, long> _offsets;
    private readonly object _lock = new();

    public SpatialIndex(string filePath)
    {
        _filePath = filePath;
        _offsets = new Dictionary<Tuple<double, double>, long>();
    }

    public void Add(double latitude, double longitude, long offset)
    {
        lock (_lock)
        {

            if (_offsets.Any())
            {
                LoadIndex();
            }

            if (_offsets.TryGetValue(new Tuple<double, double>(latitude, longitude), out var result))
            {
                if (result == offset)
                {
                    return;
                }
                _offsets[Tuple.Create(latitude, longitude)] = offset;
                ReplaceInFile(new KeyValuePair<Tuple<double, double>, long>(Tuple.Create(latitude, longitude), result), new KeyValuePair<Tuple<double, double>, long>(Tuple.Create(latitude, longitude), offset));
            }
            else
            {
                _offsets.Add(Tuple.Create(latitude, longitude), offset);
                WriteToFile(_offsets);
            }
        }
    }

    private void LoadIndex()
    {
        if (!File.Exists(_filePath)) return;

        _offsets = ReadFromFile();
    }

    public List<long> Offsets(double latitude, double longitude, double rangeInMeters, int limit)
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
                .Select(e => e.Value)
                .ToList();
        }
    }

    private void WriteToFile(Dictionary<Tuple<double, double>, long> dictionary)
    {
        using var fileStream = new FileStream(_filePath, FileMode.Create, FileAccess.Write);
        using var writer = new BinaryWriter(fileStream);
        writer.Write(dictionary.Count);

        foreach (var kvp in dictionary)
        {
            writer.Write(kvp.Key.Item1);
            writer.Write(kvp.Key.Item2);
            writer.Write(kvp.Value);
        }
    }
    private Dictionary<Tuple<double, double>, long> ReadFromFile()
    {
        var dictionary = new Dictionary<Tuple<double, double>, long>();

        using var fileStream = new FileStream(_filePath, FileMode.Open, FileAccess.Read);
        using var reader = new BinaryReader(fileStream);
        var count = reader.ReadInt32();

        for (var i = 0; i < count; i++)
        {
            var item1 = reader.ReadDouble();
            var item2 = reader.ReadDouble();
            var value = reader.ReadInt64();

            dictionary.Add(Tuple.Create(item1, item2), value);
        }

        return dictionary;
    }

    private void ReplaceInFile(KeyValuePair<Tuple<double, double>, long> oldKvp, KeyValuePair<Tuple<double, double>, long> newKvp)
    {
        var dictionary = ReadFromFile();

        if (dictionary.ContainsKey(oldKvp.Key) && dictionary[oldKvp.Key] == oldKvp.Value)
        {
            dictionary[oldKvp.Key] = newKvp.Value;
        }
        else
        {
            throw new Exception("The specified KeyValuePair was not found in the file.");
        }

        WriteToFile(dictionary);
    }
}