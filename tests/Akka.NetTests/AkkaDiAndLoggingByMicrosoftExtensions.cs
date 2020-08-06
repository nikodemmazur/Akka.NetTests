using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.DI.Core;
using Akka.DI.Extensions.DependencyInjection;
using Akka.Event;
using Akka.Logger.Extensions.Logging;
using Akka.TestKit;
using Autofac;
using Autofac.Extensions.DependencyInjection;
using Autofac.Extras.DynamicProxy;
using Castle.DynamicProxy;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NLog;
using NLog.Config;
using NLog.Extensions.Logging;
using NLog.Targets;
using Xunit;
using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace Akka.NetTests
{
    public class AkkaDiAndLoggingByMicrosoftExtensions : TestKit.Xunit2.TestKit
    {
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

        public class ParentActor : UntypedActor
        {
            private readonly IActorRef _child;

            public ParentActor()
            {
                _child = Context.ActorOf(Context.DI().Props<ChildActor>());
            }

            protected override void OnReceive(object message)
            {
                switch (message)
                {
                    case "GetChild":
                        Sender.Tell(_child);
                        break;
                    case "KillChild":
                        _child.Tell(Kill.Instance);
                        break;
                }
            }
        }

        public class ChildActor : ReceiveActor { }

        public class ActorUnawareOfLogging : UntypedActor, ILoggableActor
        {
            public ILoggingAdapter LoggingAdapter { get; } = Context.GetLogger();

            protected override void OnReceive(object message)
            {
                // Nothing here.
            }
        }

        public interface ILoggableActor
        {
            ILoggingAdapter LoggingAdapter { get; }
        }

        public class UntypedActorInterceptor : IInterceptor
        {
            private class OnReceiveProxyGenerationHook : IProxyGenerationHook
            {
                public void MethodsInspected() { }

                public void NonProxyableMemberNotification(Type type, MemberInfo memberInfo) { }

                public bool ShouldInterceptMethod(Type type, MethodInfo methodInfo) => methodInfo.Name == "OnReceive";
            }

            public static IProxyGenerationHook OnReceiveHook { get; } = new OnReceiveProxyGenerationHook();

            public void Intercept(IInvocation invocation)
            {
                ILoggingAdapter la;
                if (invocation.InvocationTarget is ILoggableActor ila)
                {
                    la = ila.LoggingAdapter;
                    la.Info($"Received message: " + invocation.Arguments.First());
                    try { invocation.Proceed(); }
                    catch (Exception ex) { la.Error(ex, "Exception thrown when processing a message."); }
                }
                else
                    throw new NullReferenceException($"The class {invocation.InvocationTarget.GetType().FullName} does not implement " +
                        $"the interface {typeof(ILoggableActor).FullName}. Cannot obtain a logger.");
            }
        }

        private readonly IServiceProvider _serviceProvider;
        public string LogDir { get; }
        public string LogPath { get; }

        private void ConfigureNlog()
        {
            var config = new LoggingConfiguration();
            var fileTarget = new FileTarget(nameof(AkkaDiAndLoggingByMicrosoftExtensions) + "Target")
            {
                FileName = LogPath,
                Layout =
                    @"[${level}][${longdate}][${stacktrace:format=Flat}]${literal:text=[Exception\: :when=length('${exception}')>0}${exception}${literal:text=]:when=length('${exception}')>0} <${message}>",
            };
            config.AddTarget(fileTarget);
            config.AddRuleForAllLevels(fileTarget);
            LogManager.Configuration = config;
        }

        public AkkaDiAndLoggingByMicrosoftExtensions()
        {
            LogDir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                nameof(AkkaDiAndLoggingByMicrosoftExtensions));
            Directory.CreateDirectory(LogDir);
            LogPath = Path.Combine(LogDir, "Akka.NetTests.Log.txt");

            var serviceCollection = new ServiceCollection();
            serviceCollection.AddLogging(builder =>
            {
                builder.AddNLog();
            });

            var containerBuilder = new ContainerBuilder();

            // Add already registered components from M.E.D.I.
            containerBuilder.Populate(serviceCollection);

            containerBuilder.RegisterType<ParentActor>().AsSelf();
            containerBuilder.RegisterType<ChildActor>().AsSelf();
            containerBuilder.RegisterType<UntypedActorInterceptor>();
            containerBuilder.RegisterType<ActorUnawareOfLogging>()
                            .EnableClassInterceptors(new ProxyGenerationOptions(UntypedActorInterceptor.OnReceiveHook))
                            .InterceptedBy(typeof(UntypedActorInterceptor));

            var container = containerBuilder.Build();

            //using var scope = container.BeginLifetimeScope("IsolatedRoot", b =>
            //{
            //b.Populate(serviceCollection, "IsolatedRoot");
            //});

            // Get IServiceProvider (Autofac) indirectly by Autofac extension.
            _serviceProvider = new AutofacServiceProvider(container);

            // Resolve ILoggerFactory and feed it into Akka logger extension static property.
            var loggerFactory = _serviceProvider.GetRequiredService<ILoggerFactory>();
            LoggingLogger.LoggerFactory = loggerFactory;
        }

        [Fact]
        public void SystemAndActorLogUsingMicrosoftExtensions()
        {
            var config = ConfigurationFactory.ParseString(@"akka {
    loggers = [""Akka.Logger.Extensions.Logging.LoggingLogger, Akka.Logger.Extensions.Logging""]
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

                var actorSystem = ActorSystem.Create("ActorSystem", config);
                var logger = Logging.GetLogger(actorSystem, actorSystem);
                logger.Info("Actor system with Microsoft Extensions created.");
                var loggingActor = actorSystem.ActorOf<LoggingActor>();
                logger.Info("Echo logging actor created.");
                loggingActor.Tell("Log this message, please.");
                loggingActor.Tell(new NullReferenceException());

                var logContent = ActorTests.WaitForFileContentAsync("Log this message, please.", LogPath, TimeSpan.FromSeconds(3)).Result;
                Assert.Contains("Actor system", logContent);
                Assert.Contains("Log this message, please.", logContent);
            }
        }

        [Fact]
        public async Task SystemAndActorSpawnActorWithDi()
        {
            var actorSystem = ActorSystem.Create("ActorSystem").UseServiceProvider(_serviceProvider);
            var parentActor = actorSystem.ActorOf(actorSystem.DI().Props<ParentActor>());

            var childActor = await parentActor.Ask<IActorRef>("GetChild");

            Assert.False(childActor.IsNobody());

            Watch(childActor);

            parentActor.Tell("KillChild");
            ExpectTerminated(childActor);

            Unwatch(childActor);

            parentActor.Tell(PoisonPill.Instance);
            await actorSystem.Terminate();
        }

        [Fact]
        public void ActorLogsViaAspectOrientedDiagnostics()
        {
            var config = ConfigurationFactory.ParseString(@"akka {
    loggers = [""Akka.Logger.Extensions.Logging.LoggingLogger, Akka.Logger.Extensions.Logging""]
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

                var actorSystem = ActorSystem.Create("ActorSystem", config).UseServiceProvider(_serviceProvider);
                var actorUnawareOfLogging = actorSystem.ActorOf(actorSystem.DI().Props<ActorUnawareOfLogging>());

                string expectedMsg = "This message should be logged by the interceptor.";

                actorUnawareOfLogging.Tell(expectedMsg);

                var logContent = ActorTests.WaitForFileContentAsync(expectedMsg, LogPath, TimeSpan.FromSeconds(20)).Result;
                Assert.Contains(expectedMsg, logContent);
            }
        }
    }
}