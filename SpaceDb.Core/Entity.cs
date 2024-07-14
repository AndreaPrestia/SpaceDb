namespace SpaceDb.Core;

public class Entity
{
    public long Timestamp { get; set; }
    public double Latitude { get; set; }
    public double Longitude { get; set; }
    public string Name { get; set; }

    public Entity(long timestamp, double latitude, double longitude, string name)
    {
        Timestamp = timestamp;
        Latitude = latitude;
        Longitude = longitude;
        Name = name;
    }
}