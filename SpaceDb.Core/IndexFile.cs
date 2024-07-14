using System.Text.Json;
using NetTopologySuite.Geometries;

namespace SpaceDb.Core;

public class IndexFile
{
    private readonly string _filePath;
    private readonly List<long> _offsets; // List of offsets in the data file

    public IndexFile(string filePath)
    {
        _filePath = filePath;
        _offsets = new List<long>();
    }

    public void AddToIndex(long offset)
    {
        _offsets.Add(offset);
    }

    public void SaveIndex()
    {
        using var stream = new FileStream(_filePath, FileMode.Create, FileAccess.Write);
        using var writer = new BinaryWriter(stream);
        foreach (var offset in _offsets)
        {
            writer.Write(offset);
        }
    }

    public void LoadIndex()
    {
        if (!File.Exists(_filePath)) return;

        using var stream = new FileStream(_filePath, FileMode.Open, FileAccess.Read);
        using var reader = new BinaryReader(stream);
        while (reader.BaseStream.Position != reader.BaseStream.Length)
        {
            var offset = reader.ReadInt64();
            _offsets.Add(offset);
        }
    }

    public List<Entity> QueryPolygon(Polygon searchPolygon, string dataFilePath)
    {
        var entities = new List<Entity>();

        foreach (var offset in _offsets)
        {
            using var stream = new FileStream(dataFilePath, FileMode.Open, FileAccess.Read);
            using var reader = new StreamReader(stream);
            reader.BaseStream.Seek(offset, SeekOrigin.Begin);
            var json = reader.ReadLine();
            if (string.IsNullOrWhiteSpace(json)) continue;

            var entity = JsonSerializer.Deserialize<Entity>(json);
            if (entity == null) continue;

            var point = new Point(entity.Longitude, entity.Latitude);

            if (searchPolygon.Contains(point))
            {
                entities.Add(entity);
            }
        }

        return entities;
    }

    public List<Entity> QueryRectangle(Rectangle searchRectangle, string dataFilePath)
    {
        var entities = new List<Entity>();

        foreach (var offset in _offsets)
        {
            using var stream = new FileStream(dataFilePath, FileMode.Open, FileAccess.Read);
            using var reader = new StreamReader(stream);
            reader.BaseStream.Seek(offset, SeekOrigin.Begin);
            var json = reader.ReadLine();
            if (string.IsNullOrWhiteSpace(json)) continue;
            var entity = JsonSerializer.Deserialize<Entity>(json);

            if (entity == null)
            {
                continue;
            }

            if (searchRectangle.Contains(entity))
            {
                entities.Add(entity);
            }
        }

        return entities;
    }
}