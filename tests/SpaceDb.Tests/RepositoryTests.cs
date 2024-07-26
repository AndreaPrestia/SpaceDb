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

        var entities = new List<City>
        {
            new()
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Latitude = 37.7749,
                Longitude = -122.4194,
                Name = "San Francisco",
                PoIs = new List<string>()
                {
                    "Golden Gate, Alcatraz, Lombard Street, Fisherman's Warf"
                }
            },
            new()
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Latitude = 34.0522,
                Longitude = -118.2437,
                Name = "Los Angeles"
            },

            new()
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Latitude = 40.7128,
                Longitude = -74.0060,
                Name = "New York",
                PoIs = new List<string>()
                {
                    "Brooklyn's Bridge, Central Park, Empire State Building, Statue of Liberty, Times Square, Broadway"
                }
            },
            new()
            {

                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Latitude =  41.8781,
                Longitude = -87.6298,
                Name = "Chicago"
            },
            new()
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Latitude = 51.5074,
                Longitude = -0.1278,
                Name = "London",
                PoIs = new List<string>()
                {
                    "London Tower, London Eye, Tower Bridge, Big Ben"
                }
            },
            new()
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Latitude =  -33.8688,
                Longitude = 151.2093,
                Name = "Sydney"
            }
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
            Assert.Equal(entityToCompare.Name, entity.Name);
            Assert.True(entityToCompare.PoIs.SequenceEqual(entity.PoIs));
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
            Assert.True(entityToCompare.PoIs.SequenceEqual(entity.PoIs));
        }, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(1);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.Equal(entityToCompare.Name, entity.Name);
            Assert.True(entityToCompare.PoIs.SequenceEqual(entity.PoIs));
        }, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(2);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.Equal(entityToCompare.Name, entity.Name);
            Assert.True(entityToCompare.PoIs.SequenceEqual(entity.PoIs));
        }, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(3);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.Equal(entityToCompare.Name, entity.Name);
            Assert.True(entityToCompare.PoIs.SequenceEqual(entity.PoIs));
        }, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(4);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.Equal(entityToCompare.Name, entity.Name);
            Assert.True(entityToCompare.PoIs.SequenceEqual(entity.PoIs));
        }, entity =>
        {
            var entityToCompare = entities.ElementAtOrDefault(5);
            Assert.NotNull(entityToCompare);
            Assert.Equal(entityToCompare.Timestamp, entity.Timestamp);
            Assert.Equal(entityToCompare.Latitude, entity.Latitude);
            Assert.Equal(entityToCompare.Longitude, entity.Longitude);
            Assert.Equal(entityToCompare.Name, entity.Name);
            Assert.True(entityToCompare.PoIs.SequenceEqual(entity.PoIs));
        });
    }

    [Fact]
    public void Add_Entity_Null_Ko()
    {
        //arrange
        var service = _host.Services.GetRequiredService<Repository>();

        //act
        void Action() => service.Add<City>(null!);

        //assert
        Assert.Throws<ArgumentNullException>(Action);
    }

    public record City : BaseEntity
    {
        public string Name { get; set; } = string.Empty;
        public List<string> PoIs { get; set; } = new();
    }
}