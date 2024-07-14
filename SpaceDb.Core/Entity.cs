namespace SpaceDb.Core;

public class Entity
{
    public long Timestamp { get; set; }
    public double Latitude { get; set; }
    public double Longitude { get; set; }

    public Entity(long timestamp, double latitude, double longitude)
    {
        Timestamp = timestamp;
        Latitude = latitude;
        Longitude = longitude;
    }
}