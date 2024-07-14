using NetTopologySuite.Geometries;
using SpaceDb.Core;

class Program
{
    static void Main(string[] args)
    {
        const string dataFilePath = "data.txt";
        const string indexFilePath = "index.dat";

        // Create an in-memory buffer and add some sample data
       // var inMemoryBuffer = new InMemoryBuffer(dataFilePath);
        var entities = new List<Entity>
        {
            new(DateTimeOffset.UtcNow.ToUnixTimeSeconds(), 37.7749, -122.4194), // San Francisco
            new(DateTimeOffset.UtcNow.ToUnixTimeSeconds(), 34.0522, -118.2437), // Los Angeles
            new(DateTimeOffset.UtcNow.ToUnixTimeSeconds(), 40.7128, -74.0060),  // New York
            new(DateTimeOffset.UtcNow.ToUnixTimeSeconds(), 41.8781, -87.6298),  // Chicago
            new(DateTimeOffset.UtcNow.ToUnixTimeSeconds(), 51.5074, -0.1278),   // London
            new(DateTimeOffset.UtcNow.ToUnixTimeSeconds(), -33.8688, 151.2093)  // Sydney
        };

        //foreach (var entity in entities)
        //{
        //    inMemoryBuffer.Add(entity);
        //}

        //// Periodically flush the in-memory buffer to file
        //inMemoryBuffer.FlushToFile();

        // Create and populate the index
        var indexFile = new IndexFile(indexFilePath);

        using (var stream = new FileStream(dataFilePath, FileMode.Open, FileAccess.Read))
        using (var reader = new BinaryReader(stream))
        {
            long offset = 0;
            while (reader.BaseStream.Position != reader.BaseStream.Length)
            {
                indexFile.AddToIndex(offset);
                reader.BaseStream.Seek(16, SeekOrigin.Current); // Skip over Timestamp
                offset = reader.BaseStream.Position;
            }
        }

        indexFile.SaveIndex();

        // Load the index
        indexFile.LoadIndex();

        // Define a polygon (e.g., a rough bounding polygon around the US mainland)
        var coordinates = new[]
        {
            new Coordinate(-130, 30),
            new Coordinate(-130, 50),
            new Coordinate(-110, 50),
            new Coordinate(-110, 30),
            new Coordinate(-130, 30)
        };
        var polygon = new Polygon(new LinearRing(coordinates));

        // Query with a polygon
        var foundEntities = indexFile.QueryPolygon(polygon, dataFilePath);

        // Print found entities for polygon query
        foreach (var entity in foundEntities)
        {
            Console.WriteLine($"Polygon Query - Found Entity: Timestamp={entity.Timestamp}, Lat={entity.Latitude}, Lon={entity.Longitude}");
        }

        Console.WriteLine("Press Enter to exit");
        Console.ReadLine();
    }
}


