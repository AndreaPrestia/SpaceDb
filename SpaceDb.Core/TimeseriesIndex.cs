namespace SpaceDb.Core;

public class TimeseriesIndex
{
    private readonly string _timeseriesIndexPath;
    private readonly Dictionary<long, List<long>> _timestampOffsets;
    private readonly object _lock = new object();
    private bool _isInitialized;

    public TimeseriesIndex(string timeseriesIndexPath)
    {
        _timeseriesIndexPath = timeseriesIndexPath;
        _timestampOffsets = new Dictionary<long, List<long>>();
    }

    public bool Initialized => _isInitialized;
    
    public void Add(long timestamp)
    {
        lock (_lock)
        {
            using (var stream = new FileStream(_timeseriesIndexPath, FileMode.Append, FileAccess.Write))
            using (var writer = new BinaryWriter(stream))
            {
                writer.Write(timestamp);

                if (!_timestampOffsets.ContainsKey(timestamp))
                {
                    _timestampOffsets.Add(timestamp, new List<long>());
                }

                _timestampOffsets[timestamp].Add(writer.BaseStream.Position);
            }
        }
    }

    public void InitIndex()
    {
        lock (_lock)
        {
            if(!_isInitialized)
            {
                long offset = 0;
                using (var stream = new FileStream(_timeseriesIndexPath, FileMode.Open, FileAccess.Read))
                using (var reader = new BinaryReader(stream))
                {
                    while (reader.BaseStream.Position != reader.BaseStream.Length)
                    {
                        var timestamp = reader.ReadInt64();

                        if (!_timestampOffsets.ContainsKey(timestamp))
                        {
                            _timestampOffsets.Add(timestamp, new List<long>());
                        }

                        _timestampOffsets[timestamp].Add(offset);

                        offset = reader.BaseStream.Position;
                    }
                }

                _isInitialized = true;
            }
        }
    }
    public List<long> Offsets(long start, long end)
    {
        lock (_lock)
        {
            return _timestampOffsets.Where(x => x.Key >= start && x.Key <= end).SelectMany(e => e.Value).ToList();
        }
    }
}

