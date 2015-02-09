using System;
using EtcdApiNet;

namespace EtcdApiNet.Test
{
    class MainClass
    {
        public static void Main (string[] args)
        {
            Console.WriteLine ("Hello World!");

            string serverListStr = "127.0.0.1:2379";
            EtcdSession session = new EtcdSession (serverListStr);

            EtcdResult result = null;

            Console.WriteLine ("======key/value======");
            Console.WriteLine ("------get------");
            result = session.Get("/foo", true);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------set------");
            result = session.SetValue("/foo", "hello world");
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------get------");
            result = session.Get("/foo", true);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------compareAndSwap1------");
            result = session.SetValue("/foo", "bar2", null, "one", 111, false);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------compareAndSwap2------");
            result = session.SetValue("/foo", "hello etcd", null, "hello world", null, true);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------get------");
            result = session.Get("/foo", true);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------compareAndDelete1------");
            result = session.DeleteValue ("/foo", "bar", 111);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------compareAndDelete2------");
            result = session.DeleteValue ("/foo", "hello etcd");
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------get------");
            result = session.Get("/foo", true);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }

            Console.WriteLine ("======dir======");
            Console.WriteLine ("------get------");
            result = session.Get("/dir", true);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------set------");
            result = session.SetDir ("/dir", 3600);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            result = session.SetValue("/dir/foo", "bar");
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------get------");
            result = session.Get("/dir", true);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------compareAndDelete1------");
            result = session.DeleteDir ("/dir", false);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------compareAndDelete2------");
            result = session.DeleteDir ("/dir", true);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------get------");
            result = session.Get("/dir", true);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }

            Console.WriteLine ("======queue======");
            Console.WriteLine ("------get------");
            result = session.Get ("/queue", false);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------enqueue------");
            result = session.Enqueue ("/queue", "job1");
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            result = session.Enqueue ("/queue", "job2", 5);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            result = session.Enqueue ("/queue", "job3");
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------get------");
            result = session.Get ("/queue", true);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------get in order------");
            result = session.Get ("/queue", true, true);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------compareAndDelete------");
            result = session.DeleteDir ("/queue", true);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
            Console.WriteLine ("------get------");
            result = session.Get("/dir", true, true);
            if (result != null)
            {
                Console.WriteLine (result.ToString ());
            }
        }
    }
}
