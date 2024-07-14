using System.Text.Json;

namespace SpaceDb.Core;

public class InMemoryBuffer
{
    private readonly List<Entity> _entities;
    private readonly string _dataFilePath;

    public InMemoryBuffer(string dataFilePath)
    {
        _entities = new List<Entity>();
        _dataFilePath = dataFilePath;
    }

    public void Add(Entity entity)
    {
        _entities.Add(entity);
    }

    public void FlushToFile()
    {
        using var stream = new FileStream(_dataFilePath, FileMode.Create, FileAccess.Write);
        using var writer = new StreamWriter(stream);
        foreach (var entity in _entities)
        {
            writer.WriteLine(JsonSerializer.Serialize(entity));
        }
    }

    public IEnumerable<Entity> GetEntities()
    {
        return _entities;
    }
}