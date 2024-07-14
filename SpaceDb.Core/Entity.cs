namespace SpaceDb.Core;

public class Entity(long timestamp, double latitude, double longitude, string name)
{
    public long Timestamp { get; set; } = timestamp;
    public double Latitude { get; set; } = latitude;
    public double Longitude { get; set; } = longitude;
    public string Name { get; set; } = name;
    public Dictionary<string, string> Properties { get; set; } = new();

    public bool SetProperty(string name, string? value)
    {
        if (Properties.ContainsKey(name))
        {
            if (string.IsNullOrEmpty(value))
            {
                return Properties.Remove(name);
            }

            Properties[name] = value;

            return true;
        }

        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        Properties.Add(name, value);

        return true;
    }

    public void WriteToBinaryWriter(BinaryWriter writer)
    {
        writer.Write(Timestamp);
        writer.Write(Latitude);
        writer.Write(Longitude);
        writer.Write(Name);
        writer.Write(Properties.Count);
        foreach (var kvp in Properties)
        {
            if (!string.IsNullOrWhiteSpace(kvp.Value))
            {
                writer.Write(kvp.Key);
                writer.Write(kvp.Value);
            }
        }
    }

    public static Entity ReadFromBinaryReader(BinaryReader reader)
    {
        long timestamp = reader.ReadInt64();
        double latitude = reader.ReadDouble();
        double longitude = reader.ReadDouble();
        string name = reader.ReadString();

        int propertyCount = reader.ReadInt32();
        var properties = new Dictionary<string, string>();
        for (int i = 0; i < propertyCount; i++)
        {
            string key = reader.ReadString();
            string value = reader.ReadString();
            properties[key] = value;
        }

        return new Entity(timestamp, latitude, longitude, name) { Properties = properties };
    }
}