using Microsoft.Extensions.Logging;
using SpaceDb.Core.Indexes;
using System.Text.Json;
using System.Text;

namespace SpaceDb.Core;

public sealed class Repository
{
    private readonly string _fileName;
    private readonly TimeSeriesIndex _timeSeriesIndex;
    private readonly SpatialIndex _spatialIndex;
    private readonly object _lock = new();
    private readonly ILogger<Repository> _logger;
    private Dictionary<string, long> _timeSeriesOffset = new();
    private Dictionary<string, long> _spatialIndexOffset = new();

    private Repository(string fileName, ILogger<Repository> logger)
    {
        _fileName = fileName;
        _logger = logger;
        _timeSeriesIndex = new TimeSeriesIndex();
        _spatialIndex = new SpatialIndex();
    }

    internal static Repository Create(string fileName, ILogger<Repository> logger)
    {
        return new Repository(fileName, logger);
    }

    /// <summary>
    /// Adds an entity in the storage
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="entity"></param>
    /// <exception cref="ArgumentNullException">If entity is null or the database file is not found</exception>
    /// <exception cref="NotSupportedException">When the entity cannot be serialized</exception>
    public void Add<T>(T entity) where T : BaseEntity
    {
        ArgumentNullException.ThrowIfNull(entity);

        lock (_lock)
        {
            var typeName = typeof(T).Name;

            using var stream = new FileStream(_fileName, FileMode.Append, FileAccess.Write);
            using var writer = new BinaryWriter(stream);
            var jsonString = JsonSerializer.Serialize(entity);
            var jsonBytes = Encoding.UTF8.GetBytes(jsonString);
            writer.Write(jsonBytes.Length);
            writer.Write(jsonBytes);
            var timeSeriesOffset = _timeSeriesOffset.ContainsKey(typeName) ? _timeSeriesOffset[typeName] : 0;
            var spatialIndexOffset = _spatialIndexOffset.ContainsKey(typeName) ? _spatialIndexOffset[typeName] : 0;
            _timeSeriesIndex.Add<T>(entity.Timestamp, timeSeriesOffset);
            _spatialIndex.Add<T>(entity.Latitude, entity.Longitude, spatialIndexOffset);

            var position = writer.BaseStream.Position;

            if (!_timeSeriesOffset.TryAdd(typeName, position))
            {
                _timeSeriesOffset[typeName] = position;
            }

            if (!_spatialIndexOffset.TryAdd(typeName, position))
            {
                _spatialIndexOffset[typeName] = position;
            }
        }
    }

    /// <summary>
    /// Finds a subset of top N records of type T in a time range
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="start"></param>
    /// <param name="end"></param>
    /// <param name="limit">if limit is less or equal than 0 or more than 5000 is used 5000 as default</param>
    /// <returns></returns>
    public IList<T> Find<T>(long start, long end, int limit) where T : BaseEntity
    {
        lock (_lock)
        {
            if (limit is <= 0 or > 5000)
            {
                limit = 5000;
            }

            List<T> entities = new();

            var offsets = _timeSeriesIndex.Offsets<T>(start, end, limit);

            if (!offsets.Any()) return entities;
            
            foreach (var offset in offsets)
            {
                try
                {
                    using var stream = new FileStream(_fileName, FileMode.Open, FileAccess.Read);
                    using var reader = new BinaryReader(stream);
                    reader.BaseStream.Seek(offset, SeekOrigin.Begin);
                    var length = reader.ReadInt32();

                    if (length <= 0) continue;
                    
                    var jsonBytes = reader.ReadBytes(length);
                    var jsonString = Encoding.UTF8.GetString(jsonBytes);
                    if (string.IsNullOrEmpty(jsonString)) continue;
                    
                    var content = JsonSerializer.Deserialize<T>(jsonString);

                    if (content == null) continue;
                    
                    entities.Add(content);
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

    /// <summary>
    /// Finds a subset of top N records of type T in a geographical range
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="latitude"></param>
    /// <param name="longitude"></param>
    /// <param name="rangeInMeters">Range in meters near the latitude/longitude pair provided. If range is less than zero it is used a default range of 10 meters</param>
    /// <param name="limit">if limit is less or equal than 0 or more than 5000 is used 5000 as default</param>
    /// <returns></returns>
    public IList<T> Find<T>(double latitude, double longitude, double rangeInMeters, int limit) where T : BaseEntity
    {
        lock (_lock)
        {
            if (limit is <= 0 or > 5000)
            {
                limit = 5000;
            }

            if (rangeInMeters < 0)
            {
                rangeInMeters = 10;
            }

            List<T> entities = new();

            var offsets = _spatialIndex.Offsets<T>(latitude, longitude, rangeInMeters, limit);

            if (!offsets.Any()) return entities;
            
            foreach (var offset in offsets)
            {
                try
                {
                    using var stream = new FileStream(_fileName, FileMode.Open, FileAccess.Read);
                    using var reader = new BinaryReader(stream);
                    reader.BaseStream.Seek(offset, SeekOrigin.Begin);
                    var length = reader.ReadInt32();

                    if (length <= 0) continue;

                    var jsonBytes = reader.ReadBytes(length);
                    var jsonString = Encoding.UTF8.GetString(jsonBytes);
                    if (string.IsNullOrEmpty(jsonString)) continue;

                    var content = JsonSerializer.Deserialize<T>(jsonString);

                    if (content == null) continue;

                    entities.Add(content);
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