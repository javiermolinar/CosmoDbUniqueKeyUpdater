﻿using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Configuration;

namespace CosmoDBCollectionPropertyUpdater
{
    static class CosmoDBCollectionPropertyUpdater
    {
        static async Task Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", true, true);
            IConfigurationRoot configuration = builder.Build();

            IDocumentClient client = new DocumentClient(
                new Uri(configuration["DatabaseUri"]),
                configuration["DatabaseToken"]);

            CollectionProperties existingCollection = new CollectionProperties();
            configuration.GetSection("CollectionProperties").Bind(existingCollection);

            string newCollectionName = existingCollection.CollectionName + "_";
            CollectionProperties newCollection = new CollectionProperties(existingCollection) {CollectionName = newCollectionName};

            Console.WriteLine($"Creating temp collection: {newCollectionName}");
            await CreateCollectionAsync(client,newCollection);
            Console.WriteLine("Copying  documents from existing collection");
            await CopyCollectionsDocumentsAsync(client, existingCollection, newCollection);
            Console.WriteLine("Restoring collection");
            await DeleteCollectionAsync(client, existingCollection);
            await CreateCollectionAsync(client, existingCollection);
            await CopyCollectionsDocumentsAsync(client, newCollection, existingCollection);
            Console.WriteLine("Removing temp collection");
            await DeleteCollectionAsync(client, newCollection);
            Console.WriteLine("Done");
            Console.WriteLine("Press any key to exit..");
            Console.ReadLine();
        }


        private static async Task CreateCollectionAsync(IDocumentClient client, CollectionProperties properties)
        {
            var newCollection = new DocumentCollection
            {
                Id = properties.CollectionName,
                DefaultTimeToLive = -1,
                IndexingPolicy = new IndexingPolicy(
                    new RangeIndex(DataType.String) { Precision = -1 },
                    new RangeIndex(DataType.Number) { Precision = -1 }
                )
            };

            if (properties.PartitionKeys.Any())
            {
                newCollection.PartitionKey.Paths = new Collection<string>(properties.PartitionKeys);
            }

            if (!string.IsNullOrEmpty(properties.ExcludedPaths))
            {
                newCollection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath{ Path = properties.ExcludedPaths });
            }

            if (properties.UniqueKeys.Any())
            {
                newCollection.UniqueKeyPolicy.UniqueKeys.Add(new UniqueKey { Paths = new Collection<string>(properties.UniqueKeys) });
            }

            await client.CreateDocumentCollectionAsync(
                UriFactory.CreateDatabaseUri(properties.DatabaseName),
                newCollection,
                new RequestOptions { OfferThroughput = properties.OfferThroughput });

        }

        private static async Task CopyCollectionsDocumentsAsync(IDocumentClient client, CollectionProperties collection1, CollectionProperties collection2)
        {

            var collection1Uri = UriFactory.CreateDocumentCollectionUri(collection1.DatabaseName, collection1.CollectionName);

            var documents = client.CreateDocumentQuery<Document>(collection1Uri)
                .AsEnumerable().ToList();

            var collection2Uri = UriFactory.CreateDocumentCollectionUri(collection2.DatabaseName, collection2.CollectionName);

            foreach (var document in documents)
            {
                await client.UpsertDocumentAsync(collection2Uri, document);
            }
        }

        private static async Task DeleteCollectionAsync(IDocumentClient client, CollectionProperties collection)
        {
            var collectionUri = UriFactory.CreateDocumentCollectionUri(collection.DatabaseName, collection.CollectionName);

            await client.DeleteDocumentCollectionAsync(collectionUri);
        }
    }
}