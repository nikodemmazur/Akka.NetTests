using System;
using System.Collections.Generic;
using System.Text;

namespace Akka.NetTests
{
    public class Locker
    {
        public static Locker Instance { get; }
        private Locker() { }
        static Locker()
        {
            Instance = new Locker();
        }
    }
}
