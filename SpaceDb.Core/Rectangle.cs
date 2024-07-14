using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SpaceDb.Core;

public struct Rectangle
{
    public double X { get; }
    public double Y { get; }
    public double Width { get; }
    public double Height { get; }

    public Rectangle(double x, double y, double width, double height)
    {
        X = x;
        Y = y;
        Width = width;
        Height = height;
    }

    public bool Contains(Entity entity)
    {
        return (entity.Longitude >= X &&
                entity.Longitude < X + Width &&
                entity.Latitude >= Y &&
                entity.Latitude < Y + Height);
    }

    public bool Intersects(Rectangle range)
    {
        return !(range.X > X + Width ||
                 range.X + range.Width < X ||
                 range.Y > Y + Height ||
                 range.Y + range.Height < Y);
    }
}