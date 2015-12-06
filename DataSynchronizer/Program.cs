namespace DataSynchronizer
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using System.Data.Entity;
    using System.Transactions;
    using System.Collections.Specialized;
    using System.Diagnostics;
    using System.Data.Common;
    using System.Data.SqlClient;

    class Program
    {
        private const int ChunkSize = 1000;

        private static StringBuilder builder = new StringBuilder();

        static void Main()
        {
            var db1 = new People1Context();

            if (db1.Database.Exists() == false)
            {
                db1.Database.Create();
                db1.Seed(50000);
            }

            var db2 = new People2Context();
            db2.Database.CreateIfNotExists();

            var s = new Stopwatch();
            s.Start();
            Sync();
            s.Stop();
            Console.WriteLine(s.Elapsed);
        }

        public static void Sync()
        {
            using (var targetConnection = new SqlConnection("Data Source=(local); Initial Catalog=People2; Integrated Security=True;"))
            {
                var src = new People1Context();
                src.Database.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);

                targetConnection.Open();
                var target = new People2Context(targetConnection);

                using (var targetTransaction = targetConnection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted))
                {
                    try
                    {
                        var totalToProcess = src.People.Count();
                        var counter = 0;
                        var currentTotal = 0;
                        while (totalToProcess > 0)
                        {
                            var peopleInSource = src.People
                                .OrderBy(x => x.Id)
                                .Skip(counter * ChunkSize)
                                .Take(ChunkSize)
                                .Select(x => x) // Map to the target entity in case they are different types
                                .ToList();

                            if (peopleInSource.Any())
                            {
                                // Since the query is ordered we can take the min and max Id by index
                                var maxId = peopleInSource[peopleInSource.Count - 1].Id;
                                var minId = peopleInSource[0].Id;
                                using (target = new People2Context(targetConnection, false))
                                {
                                    target.Database.UseTransaction(targetTransaction);
                                    target.Database.Log = l => Console.WriteLine(l);

                                    // We expect the target table to have other entries, which we should not touch but
                                    // the result might be big and fat especially if the source table is highly fragmented
                                    // that is the case when many DELETE statements have been executed for the source table 
                                    // It will not affect the overall performance that much, but it will try to eat your memory
                                    // TODO: fetch the result from target in chunks if the differences between the two tables are so dramatic
                                    //var peopleInTarget = target.People
                                    //    .OrderBy(x => x.Id)
                                    //    .Where(x => x.Id >= minId && x.Id <= maxId)
                                    //    .ToDictionary(x => x.Id);

                                    // Or do it by Ids like this, but EF is ineficcient (I think it's not true for EF 6, it will generate IN clause instead of ORs)
                                    var ids = peopleInSource.Select(x => x.Id).ToList();
                                    var peopleInTarget = target.People
                                        .Where(x => ids.Contains(x.Id))
                                        .ToDictionary(x => x.Id);

                                    //// Or with pure SQL
                                    //var peopleInTarget = target.People
                                    //    .SqlQuery(GenerateSQL(peopleInSource))
                                    //    .ToDictionary(x => x.Id);

                                    foreach (var person in peopleInSource)
                                    {
                                        if (peopleInTarget.ContainsKey(person.Id))
                                        {
                                            if (AreEqual(person, peopleInTarget[person.Id]) == false)
                                            {
                                                var personToUpdate = peopleInTarget[person.Id];
                                                target.Entry(personToUpdate).State = EntityState.Modified;
                                                
                                                OnUpdate(person, personToUpdate, target);
                                            }
                                        }
                                        else
                                        {
                                            target.Entry(person).State = EntityState.Added;
                                            
                                            OnInsert(person, target);
                                        }
                                    }
                                    target.SaveChanges();
                                }
                            }

                            counter++;
                            totalToProcess -= peopleInSource.Count();
                        }

                        targetTransaction.Commit();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(targetConnection.State);
                        targetTransaction.Rollback();
                        throw e;
                    }
                    finally
                    {
                        targetConnection.Close();
                        src.Dispose(); // This will dispose the transaction asociated with the src context
                    }
                }
            }
        }

        public static bool AreEqual(Person a, Person b)
        {
            return a.Id == b.Id &&
                a.FirstName == b.FirstName &&
                a.LastName == b.LastName &&
                a.IsStupid == b.IsStupid;
        }

        public static void OnUpdate(Person sourceEntity, Person targetEntity, People2Context targetDb)
        {
            // Don't change the Id
            // Don't dispose the db context
            // Always use this DbContext instance. 
            // Creating new instance of the DbContext will escalate the transaction to DISTRIBUTED TRANSACTION
            // which is not supported on Azure for .NET < 4.6.1

            //if (target.IsStupid == 1)
            //{
            //    //targetEntity.IsStupid = 0;
            //    // do other db logic through the context
            //}

            targetEntity.FirstName = sourceEntity.FirstName;
            targetEntity.LastName = sourceEntity.LastName;
            targetEntity.IsStupid = sourceEntity.IsStupid;
        }

        public static void OnInsert(Person entityToInsert, People2Context db)
        {
            // Don't change the Id
            // Don't dispose the db context
            // Always use this DbContext instance. 
            // Creating new instance of the DbContext will escalate the transaction to DISTRIBUTED TRANSACTION
            // which is not supported on Azure for .NET < 4.6.1
        }

        private static string GenerateSQL(IEnumerable<Person> persons)
        {
            // TODO: Fix the *
            var result = string.Format("SELECT * FROM dbo.People WHERE Id IN ({0})", 
                string.Join(",", persons.Select(x => x.Id)));
            
            return result;
        }
    }
}
