using System;
using System.Collections.Generic;
using System.Text;

namespace Akka.NetTests
{
    public class Locker
    {
        public static Locker Instance { get; }
        private Locker() { }
        // Explicit static constructor to tell C# compiler  
        // not to mark type as beforefieldinit.
        static Locker()
        {
            Instance = new Locker();
        }
    }
}
