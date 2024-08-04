namespace SpaceDb.Core.Indexes;

public class TimeSeriesIndex
{
    private readonly string _timeSeriesIndexPath = "timeSeriesIndex_{0}.db";
    private readonly Dictionary<Tuple<string, long>, List<long>> _offsets;
    private readonly object _lock = new();

    public TimeSeriesIndex()
    {
        _offsets = new Dictionary<Tuple<string, long>, List<long>>();
    }

    internal void Add<T>(long timestamp, long offset) where T : BaseEntity
    {
        lock (_lock)
        {
            if (!_offsets.Any())
            {
                LoadIndex<T>();
            }

            var typeName = typeof(T).Name;

            using var stream = new FileStream(string.Format(_timeSeriesIndexPath, typeName), FileMode.Append, FileAccess.Write);
            using var writer = new BinaryWriter(stream);
            writer.Write(timestamp);
            writer.Write(offset);

            var key = new Tuple<string, long>(typeName, timestamp);

            if (!_offsets.ContainsKey(key))
            {
                _offsets.Add(key, new List<long>()
                {
                    offset
                });
            }
            else
            {
                _offsets[key].Add(offset);
            }

        }
    }

    internal List<long> Offsets<T>(long start, long end, int limit) where T : BaseEntity
    {
        lock (_lock)
        {
            if (!_offsets.Any())
            {
                LoadIndex<T>();
            }

            var typeName = typeof(T).Name;

            return _offsets.Where(x => x.Key.Item1 == typeName && x.Key.Item2 >= start && x.Key.Item2 <= end)
                .OrderBy(x => x.Key).Take(limit)
                .SelectMany(e => e.Value)
                .ToList();
        }
    }

    private void LoadIndex<T>() where T : BaseEntity
    {
        var typeName = typeof(T).Name;
        long offset = 0;
        using var stream = new FileStream(_timeSeriesIndexPath, FileMode.OpenOrCreate, FileAccess.Read);
        using var reader = new BinaryReader(stream);
        while (reader.BaseStream.Position != reader.BaseStream.Length)
        {
            var timestamp = reader.ReadInt64();

            var key = new Tuple<string, long>(typeName, timestamp);

            if (!_offsets.ContainsKey(key))
            {
                _offsets.Add(key, new List<long>());
            }

            _offsets[key].Add(offset);

            offset = reader.BaseStream.Position;
        }
    }
}

