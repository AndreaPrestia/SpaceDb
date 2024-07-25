using SpaceDb.Core;
using SpaceDb.Core.Indexes;

namespace SpaceDb.Console;

class Program
{
    static void Main(string[] args)
    {
        const string dataFilePath = "data.dat";
        const string indexFilePath = "index.dat";
        const string timeSeriesIndexFilePath = "timeSeriesIndex.dat";

        var timeSeriesIndex = new TimeSeriesIndex(timeSeriesIndexFilePath);
        var spatialIndex = new SpatialIndex(indexFilePath);

        var repository = new Repository(dataFilePath, timeSeriesIndex, spatialIndex);
        var entities = new List<Entity>
        {
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 37.7749, -122.4194, "San Francisco"),
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 34.0522, -118.2437, "Los Angeles"),
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 40.7128, -74.0060, "New York"),
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 41.8781, -87.6298, "Chicago"),
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 51.5074, -0.1278, "London"),
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), -33.8688, 151.2093, "Sydney")
        };

        foreach (var entity in entities)
        {
            repository.Add(entity);
        }

        // Query with position
        var foundEntitiesWithPosition = repository.Find(37.7586889, -122.317707, 50000, 10);

        var end = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var start = DateTimeOffset.UtcNow.AddMinutes(3).ToUnixTimeMilliseconds();
        // Query with timestamp
        var foundEntitiesByRange = repository.Find(start, end, 10);

        // Print found entities for position query
        foreach (var entity in foundEntitiesWithPosition)
        {
            System.Console.WriteLine($"Entities by Position Query - Found Entity: Name={entity.Name} Timestamp={entity.Timestamp}, Lat={entity.Latitude}, Lon={entity.Longitude}, Properties={string.Join(", ", entity.Properties)}");
        }

        // Print found entities for timeseries query
        foreach (var entity in foundEntitiesByRange)
        {
            System.Console.WriteLine($"Entities by TimeSeries Query - Found Entity: Name={entity.Name} Timestamp={entity.Timestamp}, Lat={entity.Latitude}, Lon={entity.Longitude}, Properties={string.Join(", ", entity.Properties)}");
        }

        System.Console.WriteLine("Press Enter to exit");
        System.Console.ReadLine();
    }
}