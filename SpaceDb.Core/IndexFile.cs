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
            using var reader = new BinaryReader(stream);
            reader.BaseStream.Seek(offset, SeekOrigin.Begin);
            var timestamp = reader.ReadInt64();
            var latitude = reader.ReadDouble();
            var longitude = reader.ReadDouble();

            var point = new Point(longitude, latitude);

            if (searchPolygon.Contains(point))
            {
                var entity = new Entity(timestamp, latitude, longitude);
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
            using var reader = new BinaryReader(stream);
            reader.BaseStream.Seek(offset, SeekOrigin.Begin);
            var timestamp = reader.ReadInt64();
            var latitude = reader.ReadDouble();
            var longitude = reader.ReadDouble();

            var entity = new Entity(timestamp, latitude, longitude);
            if (searchRectangle.Contains(entity))
            {
                entities.Add(entity);
            }
        }

        return entities;
    }
}