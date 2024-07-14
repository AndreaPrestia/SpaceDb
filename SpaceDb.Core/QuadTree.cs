namespace SpaceDb.Core;

public class QuadTree
{
    private const int MaxEntities = 4;
    private const int MaxLevels = 5;

    private readonly int _level;
    private readonly List<Entity> _entities;
    private readonly Rectangle _bounds;
    private QuadTree[] _nodes;

    public QuadTree(int level, Rectangle bounds)
    {
        _level = level;
        _bounds = bounds;
        _entities = new List<Entity>();
        _nodes = new QuadTree[4];
    }

    public void Clear()
    {
        _entities.Clear();
        for (var i = 0; i < _nodes.Length; i++)
        {
            _nodes[i]?.Clear();
            _nodes[i] = null;
        }
    }

    private void Split()
    {
        var subWidth = _bounds.Width / 2;
        var subHeight = _bounds.Height / 2;
        var x = _bounds.X;
        var y = _bounds.Y;

        _nodes[0] = new QuadTree(_level + 1, new Rectangle(x + subWidth, y, subWidth, subHeight));
        _nodes[1] = new QuadTree(_level + 1, new Rectangle(x, y, subWidth, subHeight));
        _nodes[2] = new QuadTree(_level + 1, new Rectangle(x, y + subHeight, subWidth, subHeight));
        _nodes[3] = new QuadTree(_level + 1, new Rectangle(x + subWidth, y + subHeight, subWidth, subHeight));
    }

    private int GetIndex(Entity entity)
    {
        var verticalMidpoint = _bounds.X + _bounds.Width / 2;
        var horizontalMidpoint = _bounds.Y + _bounds.Height / 2;

        var topQuadrant = entity.Latitude < horizontalMidpoint;
        var bottomQuadrant = entity.Latitude >= horizontalMidpoint;

        if (entity.Longitude < verticalMidpoint)
        {
            if (topQuadrant)
            {
                return 1;
            }

            if (bottomQuadrant)
            {
                return 2;
            }
        }
        else
        {
            if (topQuadrant)
            {
                return 0;
            }

            if (bottomQuadrant)
            {
                return 3;
            }
        }

        return -1;
    }

    public void Insert(Entity entity)
    {
        if (_nodes[0] != null)
        {
            var index = GetIndex(entity);

            if (index != -1)
            {
                _nodes[index].Insert(entity);
                return;
            }
        }

        _entities.Add(entity);

        if (_entities.Count > MaxEntities && _level < MaxLevels)
        {
            if (_nodes[0] == null)
            {
                Split();
            }

            var i = 0;
            while (i < _entities.Count)
            {
                var index = GetIndex(_entities[i]);
                if (index != -1)
                {
                    _nodes[index].Insert(_entities[i]);
                    _entities.RemoveAt(i);
                }
                else
                {
                    i++;
                }
            }
        }
    }

    public List<Entity> Retrieve(List<Entity> returnEntities, Rectangle range)
    {
        if (!_bounds.Intersects(range))
        {
            return returnEntities;
        }

        foreach (var entity in _entities)
        {
            if (range.Contains(entity))
            {
                returnEntities.Add(entity);
            }
        }

        if (_nodes[0] != null)
        {
            for (var i = 0; i < _nodes.Length; i++)
            {
                _nodes[i].Retrieve(returnEntities, range);
            }
        }

        return returnEntities;
    }
}