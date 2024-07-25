using SpaceDb.Core.Indexes;
using SpaceDb.Core.Utils;

namespace SpaceDb.Core;

public sealed class Repository
{
    private readonly string _dataFilePath;
    private readonly TimeSeriesIndex _timeSeriesIndex;
    private readonly SpatialIndex _indexFile;
    private readonly object _lock = new object();

    public Repository(string dataFilePath, TimeSeriesIndex timeSeriesIndex, SpatialIndex indexFile)
    {
        _dataFilePath = dataFilePath;
        _timeSeriesIndex = timeSeriesIndex;
        _indexFile = indexFile;
    }

    public void Add(Entity entity)
    {
        lock (_lock)
        {
            using var stream = new FileStream(_dataFilePath, FileMode.Append, FileAccess.Write);
            using var writer = new BinaryWriter(stream);
            entity.WriteToBinaryWriter(writer);
            _timeSeriesIndex.Add(entity.Timestamp, writer.BaseStream.Position);
            _indexFile.Add(entity.Latitude, entity.Longitude, writer.BaseStream.Position);
        }
    }
    public IList<Entity> Find(long start, long end, int limit)
    {
        lock (_lock)
        {
            List<Entity> entities = new();

            var offsets = _timeSeriesIndex.Offsets(start, end, limit);

            if (offsets.Any())
            {
                foreach (var offset in offsets)
                {
                    try
                    {
                        using var stream = new FileStream(_dataFilePath, FileMode.Open, FileAccess.Read);
                        using var reader = new BinaryReader(stream);
                        reader.BaseStream.Seek(offset, SeekOrigin.Begin);
                        var entity = Entity.ReadFromBinaryReader(reader);
                        if (entity == null!) continue;

                        if (entity.Timestamp >= start && entity.Timestamp <= end)
                        {
                            entities.Add(entity);
                        }
                    }
                    catch (FileNotFoundException)
                    {
                        Console.Error.WriteLine($"Error: The file {_dataFilePath} does not exist.");
                    }
                    catch (IOException e)
                    {
                        Console.Error.WriteLine($"Error: An I/O error occurred. {e.Message}");
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine($"An unexpected error occurred. {e.Message}");
                    }
                }
            }
            else
            {
                try
                {
                    using var stream = new FileStream(_dataFilePath, FileMode.Open, FileAccess.Read);
                    using var reader = new BinaryReader(stream);
                    while (reader.BaseStream.Position != reader.BaseStream.Length)
                    {
                        var entity = Entity.ReadFromBinaryReader(reader);
                        if (entity == null!) continue;

                        if (entity.Timestamp >= start && entity.Timestamp <= end)
                        {
                            entities.Add(entity);
                        }
                    }
                }
                catch (FileNotFoundException)
                {
                    Console.Error.WriteLine($"Error: The file {_dataFilePath} does not exist.");
                }
                catch (IOException e)
                {
                    Console.Error.WriteLine($"Error: An I/O error occurred. {e.Message}");
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine($"An unexpected error occurred. {e.Message}");
                }
            }

            return entities;
        }
    }

    public IList<Entity> Find(double latitude, double longitude, double rangeInMeters, int limit)
    {
        lock (_lock)
        {
            List<Entity> entities = new();

            var offsets = _indexFile.Offsets(latitude, longitude, rangeInMeters, limit);

            if (offsets.Any())
            {
                foreach (var offset in offsets)
                {
                    try
                    {
                        using var stream = new FileStream(_dataFilePath, FileMode.Open, FileAccess.Read);
                        using var reader = new BinaryReader(stream);
                        reader.BaseStream.Seek(offset, SeekOrigin.Begin);
                        var entity = Entity.ReadFromBinaryReader(reader);
                        if (entity == null!) continue;

                        entities.Add(entity);
                    }
                    catch (FileNotFoundException)
                    {
                        Console.Error.WriteLine($"Error: The file {_dataFilePath} does not exist.");
                    }
                    catch (IOException e)
                    {
                        Console.Error.WriteLine($"Error: An I/O error occurred. {e.Message}");
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine($"An unexpected error occurred. {e.Message}");
                    }
                }
            }
            else
            {
                try
                {
                    using var stream = new FileStream(_dataFilePath, FileMode.Open, FileAccess.Read);
                    using var reader = new BinaryReader(stream);
                    while (reader.BaseStream.Position != reader.BaseStream.Length)
                    {
                        var entity = Entity.ReadFromBinaryReader(reader);
                        if (entity == null!) continue;

                        if (GeoUtils.HaversineDistance(latitude, longitude, entity.Latitude, entity.Longitude) <= rangeInMeters)
                        {
                            entities.Add(entity);
                        }
                    }
                }
                catch (FileNotFoundException)
                {
                    Console.Error.WriteLine($"Error: The file {_dataFilePath} does not exist.");
                }
                catch (IOException e)
                {
                    Console.Error.WriteLine($"Error: An I/O error occurred. {e.Message}");
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine($"An unexpected error occurred. {e.Message}");
                }
            }

            return entities;
        }
    }
}