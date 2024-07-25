using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace SpaceDb.Core.Extensions;

public static class DependencyInjection
{
    public static void AddSpaceDb(this IServiceCollection serviceCollection, string databaseFileName)
    {
        serviceCollection.AddSingleton(_ => Repository.Create(databaseFileName, _.GetRequiredService<ILogger<Repository>>()));
    }
}