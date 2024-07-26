using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization;
using static System.Runtime.InteropServices.JavaScript.JSType;
using System.Text.Json;
using System.Text;

namespace SpaceDb.Core;

public record Entity<T>(long Timestamp, double Latitude, double Longitude)
{
    public long Timestamp { get; set; } = Timestamp;
    public double Latitude { get; set; } = Latitude;
    public double Longitude { get; set; } = Longitude;
    public T? Content { get; set; }

    public void WriteToBinaryWriter(BinaryWriter writer)
    {
        writer.Write(Timestamp);
        writer.Write(Latitude);
        writer.Write(Longitude);
        if (Content != null)
        {
            var jsonString = JsonSerializer.Serialize(Content);
            var jsonBytes = Encoding.UTF8.GetBytes(jsonString);
            writer.Write(jsonBytes.Length);
            writer.Write(jsonBytes);
        }
        else
        {
            writer.Write(0);
        }
    }

    public static Entity<T> ReadFromBinaryReader(BinaryReader reader)
    {
        var timestamp = reader.ReadInt64();
        var latitude = reader.ReadDouble();
        var longitude = reader.ReadDouble();
        var content = default(T);

        var length = reader.ReadInt32();

        if (length > 0)
        {
            var jsonBytes = reader.ReadBytes(length);
            var jsonString = Encoding.UTF8.GetString(jsonBytes);
            if (!string.IsNullOrEmpty(jsonString))
            {
                content = JsonSerializer.Deserialize<T>(jsonString);
            }
        }

        return new Entity<T>(timestamp, latitude, longitude) { Content = content };
    }
}