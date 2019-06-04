using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Configuration;

namespace CosmoDBCollectionPropertyUpdater
{
    static class CosmoDbCollectionPropertyUpdater
    {
        static async Task Main()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", true, true);
            IConfigurationRoot configuration = builder.Build();

            IDocumentClient client = new DocumentClient(
                new Uri(configuration["CosmoDbUri"]),
                configuration["CosmoDbToken"]);

            CollectionProperties existingCollection = new CollectionProperties();
            configuration.GetSection("CollectionProperties").Bind(existingCollection);

            string newCollectionName = existingCollection.CollectionName + configuration.GetValue("TempDatabasePrefix","_");
            CollectionProperties newCollection = new CollectionProperties(existingCollection) {CollectionName = newCollectionName};

            try
            {
                Console.WriteLine($"Creating temp collection: {newCollectionName}");
                await CreateCollectionAsync(client, newCollection);
                Console.WriteLine("Copying  documents from existing collection");
                try
                {
                    await CopyCollectionsDocumentsAsync(
                        client,
                        existingCollection,
                        newCollection,
                        configuration.GetValue("SkipDuplicates",false));
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"There was an error copying documents, reverting the operation {ex}");
                    throw;
                }

                Console.WriteLine("Restoring collection");
                if (configuration.GetValue("DeleteOriginal", true))
                {
                    await DeleteCollectionAsync(client, existingCollection);
                    await CreateCollectionAsync(client, existingCollection);
                    await CopyCollectionsDocumentsAsync(client, newCollection, existingCollection);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"There has been an error {ex.InnerException}");
            }
            
            finally
            {
                Console.WriteLine("Removing temp collection");
                await DeleteCollectionAsync(client, newCollection);
            }         
            
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

        private static async Task CopyCollectionsDocumentsAsync(IDocumentClient client, CollectionProperties collection1, CollectionProperties collection2, bool skipDuplicates = false)
        {

           var collection1Uri = UriFactory.CreateDocumentCollectionUri(collection1.DatabaseName, collection1.CollectionName);
           var collection2Uri = UriFactory.CreateDocumentCollectionUri(collection2.DatabaseName, collection2.CollectionName);

           var documents = client.CreateDocumentQuery<Document>(collection1Uri)
                .AsEnumerable().ToList();           

           foreach (var document in documents)
           {
               try
               {
                   await client.UpsertDocumentAsync(collection2Uri, document);
               }
               catch (DocumentClientException ex)
               {
                   Console.WriteLine("Found a duplicate");
                   if (!skipDuplicates)
                   {
                       throw;
                   }
               }
               
           }
           var documents2 = client.CreateDocumentQuery<Document>(collection2Uri)
                .AsEnumerable().ToList();          

            if(documents.Count != documents2.Count)
            {
                Console.WriteLine($"There was a mismatch the original collection has {documents.Count} documents while the new one has {documents2.Count}");
                Console.WriteLine("Press c to cancel or other key to continue");
                var key = Console.ReadKey();
                if (key.KeyChar == 'c')
                {
                    throw new Exception();
                }
            }
        }

        private static async Task DeleteCollectionAsync(IDocumentClient client, CollectionProperties collection)
        {
            var collectionUri = UriFactory.CreateDocumentCollectionUri(collection.DatabaseName, collection.CollectionName);

            await client.DeleteDocumentCollectionAsync(collectionUri);
        }
    }
}
