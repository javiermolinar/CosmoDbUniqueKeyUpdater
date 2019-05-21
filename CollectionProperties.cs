using System.Collections.Generic;


namespace CosmoDBCollectionPropertyUpdater
{
    public class CollectionProperties
    {
        public string DatabaseName { get; set; }
        public string CollectionName { get; set; }
        public List<string>  PartitionKeys { get; set; }
        public string ExcludedPaths { get; set; }
        public List<string> UniqueKeys { get; set; }
        public int OfferThroughput { get; set; }

        public CollectionProperties()
        {
            PartitionKeys = new List<string>();
            UniqueKeys = new List<string>();
        }

        public CollectionProperties(CollectionProperties copyCollection)
        {
            DatabaseName = copyCollection.DatabaseName;
            CollectionName = copyCollection.CollectionName;
            PartitionKeys = new List<string>(copyCollection.PartitionKeys);
            ExcludedPaths = copyCollection.ExcludedPaths;
            UniqueKeys = new List<string>(copyCollection.UniqueKeys);
            OfferThroughput = copyCollection.OfferThroughput;
        }
    }
}
