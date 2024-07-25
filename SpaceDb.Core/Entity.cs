namespace SpaceDb.Core;

public record Entity(long Timestamp, double Latitude, double Longitude, string Name)
{
    public long Timestamp { get; set; } = Timestamp;
    public double Latitude { get; set; } = Latitude;
    public double Longitude { get; set; } = Longitude;
    public string Name { get; set; } = Name;
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
        foreach (var kvp in Properties.Where(kvp => !string.IsNullOrWhiteSpace(kvp.Value)))
        {
            writer.Write(kvp.Key);
            writer.Write(kvp.Value);
        }
    }

    public static Entity ReadFromBinaryReader(BinaryReader reader)
    {
        var timestamp = reader.ReadInt64();
        var latitude = reader.ReadDouble();
        var longitude = reader.ReadDouble();
        var name = reader.ReadString();

        var propertyCount = reader.ReadInt32();
        var properties = new Dictionary<string, string>();
        for (var i = 0; i < propertyCount; i++)
        {
            var key = reader.ReadString();
            var value = reader.ReadString();
            properties[key] = value;
        }

        return new Entity(timestamp, latitude, longitude, name) { Properties = properties };
    }
}