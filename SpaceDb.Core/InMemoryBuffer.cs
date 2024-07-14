using System;

namespace SpaceDb.Core;

public class InMemoryBuffer
{
    private readonly List<Entity> _entities;
    private readonly string _dataFilePath;
    private readonly object _lock = new object();

    public InMemoryBuffer(string dataFilePath)
    {
        _entities = new List<Entity>();
        _dataFilePath = dataFilePath;
    }

    public void Add(Entity entity)
    {
        lock (_lock)
        {
            _entities.Add(entity);
        }
    }

    public void FlushToFile()
    {
        lock (_lock)
        {
            using (var stream = new FileStream(_dataFilePath, FileMode.Append, FileAccess.Write))
            using (var writer = new BinaryWriter(stream))
            {
                foreach (var entity in _entities)
                {
                    entity.WriteToBinaryWriter(writer);
                }
                _entities.Clear();
            }
        }
    }

    public IEnumerable<Entity> GetEntities()
    {
        lock (_lock)
        {
            return _entities;
        }
    }

    public IEnumerable<Entity> Query(long start, long end)
    {
        lock (_lock)
        {
            return _entities.Where(x => x.Timestamp >= start && x.Timestamp <= end);
        }
    }
}