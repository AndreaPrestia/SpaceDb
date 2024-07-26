# SpaceDb

A study project on spatial database based in .NET.

The purpose of this project is to study how to implement a solution that is data space-oriented.

I need something that is lightweight, highly portable, easily scalable and that supports GIS searches.

It should implement [CQRS](https://martinfowler.com/bliki/CQRS.html) and must be schema less.

It can be useful to track events of different manners.

## Schema
No schema should be defined. At most we can define some sort of data organizations by partion-key or collection.

## How to use it

If you want to try it just download this repo and add this line of code in your program:

```
    services.AddSpaceDb("SpatialData.dat");
```

The **SpaceDb.Core.Extensions** exposes an extension method called **AddSpaceDb** that has the database name as parameter.

If you want to use the **Repository** class just inject it where you want or retrieve it from **ServiceProvider**.

## Repository

This class exposes the following methods:

    - Add<T>(T entity)
    - Find<T>(long start, long end, int limit)
    - Find<T>(double latitude, double longitude, double rangeInMeters, int limit)

### Add<T>(T entity)

The **Add<T>** method adds a record of type T in the storage. It must derive from **BaseEntity** abstract record.

### Find<T>(long start, long end, int limit)

This method finds a subset of **T** records that have the timestamp between **start** and **end** passed as parameters. 
It must derive from **BaseEntity** abstract record.
The **limit** parameter is used to limit the number of entities returned.

### Find<T>(double latitude, double longitude, double rangeInMeters, int limit)

This method finds a subset of **T** records that are in the **rangeInMeters** of the **latitude** and **longitude** passed as parameters.
It must derive from **BaseEntity** abstract record.
The **limit** parameter is used to limit the number of entities returned.

## TODO

    - Better unit testing,
    - Implement a more robust way to manage read/write.
    - Implement delete function.
    - Implement partition-key/collection management.
    - Implement search by properties in T with equals and like.