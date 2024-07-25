using Microsoft.Extensions.Logging;
using SpaceDb.Core.Indexes;

namespace SpaceDb.Core;

public sealed class Repository
{
    private readonly string _fileName;
    private readonly TimeSeriesIndex _timeSeriesIndex;
    private readonly SpatialIndex _spatialIndex;
    private readonly object _lock = new();
    private readonly ILogger<Repository> _logger;
    private long _timeSeriesOffset = 0;
    private long _spatialIndexOffset = 0;

    private Repository(string fileName, ILogger<Repository> logger)
    {
        _fileName = fileName;
        _logger = logger;
        _timeSeriesIndex = new TimeSeriesIndex("timeSeriesIndex.dat");
        _spatialIndex = new SpatialIndex("spatialIndex.dat");
    }

    internal static Repository Create(string fileName, ILogger<Repository> logger)
    {
        return new Repository(fileName, logger);
    }

    public void Add(Entity entity)
    {
        lock (_lock)
        {
            using var stream = new FileStream(_fileName, FileMode.Append, FileAccess.Write);
            using var writer = new BinaryWriter(stream);
            entity.WriteToBinaryWriter(writer);
            _timeSeriesIndex.Add(entity.Timestamp, _timeSeriesOffset);
            _spatialIndex.Add(entity.Latitude, entity.Longitude, _spatialIndexOffset);
            _timeSeriesOffset = writer.BaseStream.Position;
            _spatialIndexOffset = writer.BaseStream.Position;
        }
    }

    public IList<Entity> Find(long start, long end, int limit)
    {
        lock (_lock)
        {
            List<Entity> entities = new();

            var offsets = _timeSeriesIndex.Offsets(start, end, limit);

            if (!offsets.Any()) return entities;
            
            foreach (var offset in offsets)
            {
                try
                {
                    using var stream = new FileStream(_fileName, FileMode.Open, FileAccess.Read);
                    using var reader = new BinaryReader(stream);
                    reader.BaseStream.Seek(offset, SeekOrigin.Begin);
                    var entity = Entity.ReadFromBinaryReader(reader);
                    if (entity == null!) continue;

                    entities.Add(entity);
                }
                catch (FileNotFoundException)
                {
                    _logger.LogError($"Error: The file {_fileName} does not exist.");
                }
                catch (IOException e)
                {
                    _logger.LogError($"Error: An I/O error occurred. {e.Message}");
                }
                catch (Exception e)
                {
                    _logger.LogError($"An unexpected error occurred. {e.Message}");
                }
            }

            return entities.OrderBy(e => e.Timestamp).ToList();
        }
    }

    public IList<Entity> Find(double latitude, double longitude, double rangeInMeters, int limit)
    {
        lock (_lock)
        {
            List<Entity> entities = new();

            var offsets = _spatialIndex.Offsets(latitude, longitude, rangeInMeters, limit);

            if (!offsets.Any()) return entities;
            
            foreach (var offset in offsets)
            {
                try
                {
                    using var stream = new FileStream(_fileName, FileMode.Open, FileAccess.Read);
                    using var reader = new BinaryReader(stream);
                    reader.BaseStream.Seek(offset, SeekOrigin.Begin);
                    var entity = Entity.ReadFromBinaryReader(reader);
                    if (entity == null!) continue;

                    entities.Add(entity);
                }
                catch (FileNotFoundException)
                {
                    _logger.LogError($"Error: The file {_fileName} does not exist.");
                }
                catch (IOException e)
                {
                    _logger.LogError($"Error: An I/O error occurred. {e.Message}");
                }
                catch (Exception e)
                {
                    _logger.LogError($"An unexpected error occurred. {e.Message}");
                }
            }

            return entities;
        }
    }
}