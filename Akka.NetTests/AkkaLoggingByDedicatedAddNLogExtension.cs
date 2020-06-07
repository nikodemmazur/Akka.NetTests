using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Cluster.Tools.Client;
using Akka.Configuration;
using Akka.Configuration.Hocon;
using Akka.Event;
using NLog;
using NLog.Config;
using NLog.Targets;
using Xunit;

namespace Akka.NetTests
{
    public class AkkaLoggingByDedicatedAddNLogExtension : TestKit.Xunit2.TestKit
    {
        public string LogDir { get; }

        public string LogPath { get; }

        private void ConfigureNlog()
        {
            var config = new LoggingConfiguration();
            var fileTarget = new FileTarget(nameof(AkkaLoggingByDedicatedAddNLogExtension) + "Target")
            {
                FileName = LogPath,
                Layout =
                    @"[${level}][${longdate}][${stacktrace:format=Flat}]${literal:text=[Exception\: :when=length('${exception}')>0}${exception}${literal:text=]:when=length('${exception}')>0} <${message}>",
            };
            config.AddTarget(fileTarget);
            config.AddRuleForAllLevels(fileTarget);
            LogManager.Configuration = config;
        }

        public AkkaLoggingByDedicatedAddNLogExtension()
        {
            LogDir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                nameof(AkkaLoggingByDedicatedAddNLogExtension));
            Directory.CreateDirectory(LogDir);
            LogPath = Path.Combine(LogDir, "Akka.NetTests.Log.txt");
        }

        public class LoggingActor : UntypedActor
        {
            private readonly ILoggingAdapter _logger = Logging.GetLogger(Context);
            protected override void OnReceive(object message)
            {
                if (message is Exception ex)
                    _logger.Error(ex, "Got exception.");
                else
                    _logger.Info("Got message: {0}", message);
            }
        }

        [Fact]
        public void SystemAndActorLogUsingNLogLoggingForAkka()
        {
            var config = ConfigurationFactory.ParseString(@"akka {
    loggers = [""Akka.Logger.NLog.NLogLogger, Akka.Logger.NLog""]
    loglevel = info
    log-config-on-start = off
    stdout-loglevel = off
    actor {
      debug {
        receive = on      # log any received message
        autoreceive = on  # log automatically received messages, e.g. PoisonPill
        lifecycle = on    # log actor lifecycle changes
        event-stream = on # log subscription changes for Akka.NET event stream
        unhandled = on    # log unhandled messages sent to actors
      }
    }
}");
            lock (Locker.Instance)
            {
                ConfigureNlog();

                if (File.Exists(LogPath))
                    File.Delete(LogPath);

                var actorSystem = ActorSystem.Create("MyActorSystem", config);
                var logger = Logging.GetLogger(actorSystem, actorSystem);
                logger.Info("Actor system created.");
                var loggingActor = actorSystem.ActorOf<LoggingActor>();
                logger.Info("Echo logging actor created.");
                loggingActor.Tell("Log this message, please.");
                loggingActor.Tell(new NullReferenceException());

                var logContent = ActorTests.WaitForFileContentAsync("Log this message, please.", LogPath, TimeSpan.FromSeconds(30)).Result;
                Assert.Contains("Actor system created.", logContent);
                Assert.Contains("Log this message, please.", logContent);
            }
        }
    }
}
