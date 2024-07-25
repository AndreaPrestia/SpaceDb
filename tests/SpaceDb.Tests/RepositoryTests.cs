using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SpaceDb.Core;
using SpaceDb.Core.Extensions;

namespace SpaceDb.Tests;

public class RepositoryTests
{
    private readonly IHost _host;
    private readonly string _fileName = "testData.dat";

    public RepositoryTests()
    {
        _host = Host.CreateDefaultBuilder()
            .ConfigureServices((_, services) =>
            {
                services.AddSpaceDb(_fileName);
            })
            .Build();
    }

    [Fact]
    public void Flow_Ok()
    {
        //arrange
        if (File.Exists(_fileName))
        {
            File.Delete(_fileName);
        }

        if (File.Exists("timeSeriesIndex.dat"))
        {
            File.Delete("timeSeriesIndex.dat");
        }

        if (File.Exists("spatialIndex.dat"))
        {
            File.Delete("spatialIndex.dat");
        }

        var entities = new List<Entity>
        {
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 37.7749, -122.4194, "San Francisco"),
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 34.0522, -118.2437, "Los Angeles"),
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 40.7128, -74.0060, "New York"),
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 41.8781, -87.6298, "Chicago"),
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 51.5074, -0.1278, "London"),
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), -33.8688, 151.2093, "Sydney")
        };

        var repository = _host.Services.GetRequiredService<Repository>();

        //act
        foreach (var entity in entities)
        {
            repository.Add(entity);
        }

        // Query with position
        var foundEntitiesWithPosition = repository.Find(37.7586889, -122.317707, 50000, 10);

        var end = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var start = DateTimeOffset.UtcNow.AddMinutes(-3).ToUnixTimeMilliseconds();
        // Query with timestamp
        var foundEntitiesByRange = repository.Find(start, end, 10);

        //assert
        Assert.NotNull(foundEntitiesWithPosition);
        Assert.Collection(foundEntitiesWithPosition, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(0);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.Equal(entityToCompare.Name, entity.Name);
        });

        Assert.NotNull(foundEntitiesByRange);
        Assert.Collection(foundEntitiesByRange, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(0);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.Equal(entityToCompare.Name, entity.Name);
        }, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(1);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.Equal(entityToCompare.Name, entity.Name);
        }, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(2);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.Equal(entityToCompare.Name, entity.Name);
        }, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(3);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.Equal(entityToCompare.Name, entity.Name);
        }, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(4);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.Equal(entityToCompare.Name, entity.Name);
        }, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(5);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.Equal(entityToCompare.Name, entity.Name);
        });
    }
}