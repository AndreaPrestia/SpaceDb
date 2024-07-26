namespace SpaceDb.Core;

public abstract record BaseEntity
{
    public long Timestamp { get; set; }
    public double Latitude { get; set; }
    public double Longitude { get; set; }
}