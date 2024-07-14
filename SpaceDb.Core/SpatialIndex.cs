using NetTopologySuite.Geometries;
using NetTopologySuite.Index.Strtree;

namespace SpaceDb.Core;

public class SpatialIndex
{
    private readonly STRtree<Entity> _rtree;
    private readonly QuadTree _quadTree;

    public SpatialIndex(Rectangle bounds)
    {
        _rtree = new STRtree<Entity>();
        _quadTree = new QuadTree(0, bounds);
    }

    public void AddEntity(Entity entity)
    {
        var point = new Point(entity.Longitude, entity.Latitude);
        _rtree.Insert(point.EnvelopeInternal, entity);
        _quadTree.Insert(entity);
    }

    public List<Entity> QueryRectangle(Rectangle rectangle)
    {
        return _quadTree.Retrieve(new List<Entity>(), rectangle);
    }

    public List<Entity> QueryPolygon(Polygon polygon)
    {
        var results = _rtree.Query(polygon.EnvelopeInternal);
        var foundEntities = new List<Entity>();

        foreach (var entity in results)
        {
            var point = new Point(entity.Longitude, entity.Latitude);
            if (polygon.Contains(point))
            {
                foundEntities.Add(entity);
            }
        }

        return foundEntities;
    }
}