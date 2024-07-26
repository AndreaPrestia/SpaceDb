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

        var entities = new List<Entity<City>>
        {
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 37.7749, -122.4194)
            {
                Content = new City()
                {
                    Name = "San Francisco",
                    PoIs = new List<string>()
                    {
                        "Golden Gate, Alcatraz, Lombard Street, Fisherman's Warf"
                    }
                }
            },
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 34.0522, -118.2437)
            {
                Content = new City()
                {
                    Name = "Los Angeles"
                }
            },
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 40.7128, -74.0060)    {
                Content = new City()
                {
                    Name = "New York",
                    PoIs = new List<string>()
                    {
                        "Brooklyn's Bridge, Central Park, Empire State Building, Statue of Liberty, Times Square, Broadway"
                    }
                }
            },
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 41.8781, -87.6298)
            {
                Content = new City()
                {
                    Name = "Chicago"
                }
            },
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 51.5074, -0.1278)
            {
                Content = new City()
                {
                    Name = "London",
                    PoIs = new List<string>()
                    {
                        "London Tower, London Eye, Tower Bridge, Big Ben"
                    }
                }
            },
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), -33.8688, 151.2093)
            {
                Content = new City()
                {
                    Name = "Sydney"
                }
            },
            new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), -31.8683, 152.2094)
        };

        var repository = _host.Services.GetRequiredService<Repository>();

        //act
        foreach (var entity in entities)
        {
            repository.Add(entity);
        }

        // Query with position
        var foundEntitiesWithPosition = repository.Find<City>(37.7586889, -122.317707, 50000, 10);

        var end = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var start = DateTimeOffset.UtcNow.AddMinutes(-5).ToUnixTimeMilliseconds();
        // Query with timestamp
        var foundEntitiesByRange = repository.Find<City>(start, end, 10);

        //assert
        Assert.NotNull(foundEntitiesWithPosition);
        Assert.Collection(foundEntitiesWithPosition, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(0);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.NotNull(entity.Content);
            Assert.NotNull(entityToCompare.Content);
            Assert.Equal(entityToCompare.Content.Name, entity.Content.Name);
            Assert.True(entityToCompare.Content.PoIs.SequenceEqual(entity.Content.PoIs));
        });

        Assert.NotNull(foundEntitiesByRange);
        Assert.Collection(foundEntitiesByRange, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(0);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.NotNull(entity.Content);
            Assert.NotNull(entityToCompare.Content);
            Assert.Equal(entityToCompare.Content.Name, entity.Content.Name);
            Assert.True(entityToCompare.Content.PoIs.SequenceEqual(entity.Content.PoIs));
        }, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(1);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.NotNull(entity.Content);
            Assert.NotNull(entityToCompare.Content);
            Assert.Equal(entityToCompare.Content.Name, entity.Content.Name);
            Assert.True(entityToCompare.Content.PoIs.SequenceEqual(entity.Content.PoIs));
        }, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(2);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.NotNull(entity.Content);
            Assert.NotNull(entityToCompare.Content);
            Assert.Equal(entityToCompare.Content.Name, entity.Content.Name);
            Assert.True(entityToCompare.Content.PoIs.SequenceEqual(entity.Content.PoIs));
        }, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(3);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.NotNull(entity.Content);
            Assert.NotNull(entityToCompare.Content);
            Assert.Equal(entityToCompare.Content.Name, entity.Content.Name);
            Assert.True(entityToCompare.Content.PoIs.SequenceEqual(entity.Content.PoIs));
        }, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(4);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.NotNull(entity.Content);
            Assert.NotNull(entityToCompare.Content);
            Assert.Equal(entityToCompare.Content.Name, entity.Content.Name);
            Assert.True(entityToCompare.Content.PoIs.SequenceEqual(entity.Content.PoIs));
        }, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(5);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.NotNull(entity.Content);
            Assert.NotNull(entityToCompare.Content);
            Assert.Equal(entityToCompare.Content.Name, entity.Content.Name);
            Assert.True(entityToCompare.Content.PoIs.SequenceEqual(entity.Content.PoIs));
        }, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(6);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.Null(entity.Content);
            Assert.Null(entityToCompare.Content);
        });
    }

    public class City
    {
        public string Name { get; set; } = string.Empty;
        public List<string> PoIs { get; set; } = new();
    }
}