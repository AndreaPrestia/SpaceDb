using Microsoft.Extensions.Logging;

namespace SpaceDb.Core
{
    public sealed class Repository
    {
        private readonly string _dataFilePath;
        private readonly TimeseriesIndex _timeSeriesIndex;
        private readonly SpatialIndex _spatialIndex;
        private readonly ILogger<Repository> _logger;

        public Repository(string dataFilePath, TimeseriesIndex timeSeriesIndex, SpatialIndex spatialIndex, ILogger<Repository> logger)
        {
            _dataFilePath = dataFilePath;
            _timeSeriesIndex = timeSeriesIndex;
            _spatialIndex = spatialIndex;
            _logger = logger;
        }

        public IList<Entity> Find(long start, long end)
        {
            if (!_timeSeriesIndex.Initialized)
            {
                _timeSeriesIndex.InitIndex();
            }

            List<Entity> entities = new List<Entity>();

            var offsets = _timeSeriesIndex.Offsets(start, end);

            if(offsets != null && offsets.Any()) 
            {
                foreach (var offset in offsets)
                {
                    try
                    {
                        using (var stream = new FileStream(_dataFilePath, FileMode.Open, FileAccess.Read))
                        using (var reader = new BinaryReader(stream))
                        {
                            reader.BaseStream.Seek(offset, SeekOrigin.Begin);
                            var entity = Entity.ReadFromBinaryReader(reader);
                            if (entity == null) continue;

                            if (entity.Timestamp >= start && entity.Timestamp <= end)
                            {
                                entities.Add(entity);
                            }
                        }

                    }
                    catch (FileNotFoundException)
                    {
                        _logger.LogError($"Error: The file {_dataFilePath} does not exist.");
                    }
                    catch (IOException e)
                    {
                        _logger.LogError($"Error: An I/O error occurred. {e.Message}");
                    }
                    catch (Exception e)
                    {
                        _logger.LogError($"An unexpected error occurred. {e.Message}");
                    }
                }
            }
            else
            {
                try
                {
                    using (var stream = new FileStream(_dataFilePath, FileMode.Open, FileAccess.Read))
                    using (var reader = new BinaryReader(stream))
                    {
                        while (reader.BaseStream.Position != reader.BaseStream.Length)
                        {
                            var entity = Entity.ReadFromBinaryReader(reader);
                            if (entity == null) continue;

                            if (entity.Timestamp >= start && entity.Timestamp <= end)
                            {
                                entities.Add(entity);
                            }
                        }
                    }

                }
                catch (FileNotFoundException)
                {
                    _logger.LogError($"Error: The file {_dataFilePath} does not exist.");
                }
                catch (IOException e)
                {
                    _logger.LogError($"Error: An I/O error occurred. {e.Message}");
                }
                catch (Exception e)
                {
                    _logger.LogError($"An unexpected error occurred. {e.Message}");
                }
            }

            return entities;
        }
    }
}
