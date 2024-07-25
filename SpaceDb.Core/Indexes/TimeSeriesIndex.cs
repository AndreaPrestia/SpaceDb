namespace SpaceDb.Core.Indexes;

public class TimeSeriesIndex
{
    private readonly string _timeSeriesIndexPath;
    private readonly Dictionary<long, List<long>> _offsets;
    private readonly object _lock = new();

    public TimeSeriesIndex(string timeSeriesIndexPath)
    {
        _timeSeriesIndexPath = timeSeriesIndexPath;
        _offsets = new Dictionary<long, List<long>>();
    }

    public void Add(long timestamp, long offset)
    {
        lock (_lock)
        {
            if (_offsets.Any())
            {
                LoadIndex();
            }

            using var stream = new FileStream(_timeSeriesIndexPath, FileMode.Append, FileAccess.Write);
            using var writer = new BinaryWriter(stream);
            writer.Write(timestamp);
            writer.Write(offset);

            if (!_offsets.ContainsKey(timestamp))
            {
                _offsets.Add(timestamp, new List<long>()
                {
                    offset
                });
            }
            else
            {
                _offsets[timestamp].Add(offset);
            }

        }
    }

    public List<long> Offsets(long start, long end, int limit)
    {
        lock (_lock)
        {
            if (!_offsets.Any())
            {
                LoadIndex();
            }

            return _offsets.Where(x => x.Key >= start && x.Key <= end)
                .OrderBy(x => x.Key).Take(limit)
                .SelectMany(e => e.Value)
                .ToList();
        }
    }

    private void LoadIndex()
    {
        long offset = 0;
        using var stream = new FileStream(_timeSeriesIndexPath, FileMode.Open, FileAccess.Read);
        using var reader = new BinaryReader(stream);
        while (reader.BaseStream.Position != reader.BaseStream.Length)
        {
            var timestamp = reader.ReadInt64();

            if (!_offsets.ContainsKey(timestamp))
            {
                _offsets.Add(timestamp, new List<long>());
            }

            _offsets[timestamp].Add(offset);

            offset = reader.BaseStream.Position;
        }
    }
}

