using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EntityFramework.Utilities;
using System.Data.Objects;
using System.Data.Common;

namespace DataSynchronizer
{
    public class People1Context : DbContext
    {
        public People1Context(DbConnection connection, bool contextOwnsConnection = false)
            : base(connection, contextOwnsConnection)
        {
            
        }

        public People1Context()
            : base("Data Source=(local); Initial Catalog=People1; Integrated Security=True;")
        {

        }

        public void Seed(int count)
        {
            var list = new List<Person>();
            for (int i = 1; i <= count; i++)
            {
                var p = new Person
                {
                    FirstName = "Peter " + i,
                    LastName = "Goter " + i,
                    IsStupid = true
                };
                list.Add(p);
            }

            EFBatchOperation.For(this, this.People).InsertAll(list);
        }

        public void Destroy()
        {
            this.Database.ExecuteSqlCommand("TRUNCATE TABLE dbo.People1");
        }

        public DbSet<Person> People { get; set; }
    }

    public class People2Context : DbContext
    {
        public People2Context(DbConnection connection, bool contextOwnsConnection = false)
            : base(connection, contextOwnsConnection)
        {
            
        }

        public People2Context()
            : base("Data Source=(local); Initial Catalog=People2; Integrated Security=True;")
        {
            
        }

        public void Destroy()
        {
            this.Database.ExecuteSqlCommand("TRUNCATE TABLE dbo.People2");
        }

        public DbSet<Person> People { get; set; }
    }

    public class Person
    {
        public int Id { get; set; }

        public string FirstName { get; set; }

        public string LastName { get; set; }

        public bool IsStupid { get; set; }
    }
}
