(ns net.zopg.zo.alpha.bench.connection
  (:require
   [clojure.core.async :refer [<!!]]
   [net.zopg.zo.test.config :refer [conn-params]]
   [net.zopg.zo.async.alpha :as zo]))

(defn connect-and-close []
  (let [client (zo/client conn-params)]
    (with-open [sess (<!! (zo/connect client))]
      (do))))

;; currently select-1 doesn't work
;; one of two errors:

;; Exception in thread "async-dispatch-3" java.lang.Error: io.netty.channel.AbstractChannel$AnnotatedSocketException: Address already in use: localhost/127.0.0.1:5496
;; 	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1148)
;; 	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
;; 	at java.lang.Thread.run(Thread.java:745)
;; Caused by: io.netty.channel.AbstractChannel$AnnotatedSocketException: Address already in use: localhost/127.0.0.1:5496
;; 	at sun.nio.ch.Net.connect0(Native Method)
;; 	at sun.nio.ch.Net.connect(Net.java:458)
;; 	at sun.nio.ch.Net.connect(Net.java:450)
;; 	at sun.nio.ch.SocketChannelImpl.connect(SocketChannelImpl.java:648)
;; 	at io.netty.util.internal.SocketUtils$3.run(SocketUtils.java:83)
;; 	at io.netty.util.internal.SocketUtils$3.run(SocketUtils.java:80)
;; 	at java.security.AccessController.doPrivileged(Native Method)
;; 	at io.netty.util.internal.SocketUtils.connect(SocketUtils.java:80)
;; 	at io.netty.channel.socket.nio.NioSocketChannel.doConnect(NioSocketChannel.java:337)
;; 	at io.netty.channel.nio.AbstractNioChannel$AbstractNioUnsafe.connect(AbstractNioChannel.java:254)
;; 	at io.netty.channel.DefaultChannelPipeline$HeadContext.connect(DefaultChannelPipeline.java:1266)
;; 	at io.netty.channel.AbstractChannelHandlerContext.invokeConnect(AbstractChannelHandlerContext.java:545)
;; 	at io.netty.channel.AbstractChannelHandlerContext.connect(AbstractChannelHandlerContext.java:530)
;; 	at aleph.tcp$client_channel_handler$reify__16241.connect(tcp.clj:96)
;; 	at io.netty.channel.AbstractChannelHandlerContext.invokeConnect(AbstractChannelHandlerContext.java:545)
;; 	at io.netty.channel.AbstractChannelHandlerContext.connect(AbstractChannelHandlerContext.java:530)
;; 	at io.netty.channel.AbstractChannelHandlerContext.connect(AbstractChannelHandlerContext.java:512)
;; 	at io.netty.channel.DefaultChannelPipeline.connect(DefaultChannelPipeline.java:985)
;; 	at io.netty.channel.AbstractChannel.connect(AbstractChannel.java:255)
;; 	at io.netty.bootstrap.Bootstrap$3.run(Bootstrap.java:252)
;; 	at io.netty.util.concurrent.AbstractEventExecutor.safeExecute(AbstractEventExecutor.java:163)
;; 	at io.netty.util.concurrent.SingleThreadEventExecutor.runAllTasks(SingleThreadEventExecutor.java:403)
;; 	at io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:442)
;; 	at io.netty.util.concurrent.SingleThreadEventExecutor$5.run(SingleThreadEventExecutor.java:858)
;; 	at io.netty.util.concurrent.DefaultThreadFactory$DefaultRunnableDecorator.run(DefaultThreadFactory.java:144)
;; 	... 1 more
;; Caused by: java.net.BindException: Address already in use
;; 	... 26 more


;; Exception in thread "async-dispatch-6" java.lang.IllegalArgumentException: No matching clause: :row-description
;; 	at net.zopg.zo.session.impl$eval18337$fn__18339.invoke(impl.clj:172)
;; 	at clojure.lang.MultiFn.invoke(MultiFn.java:233)
;; 	at net.zopg.zo.session.impl$state_loop$fn__18234$state_machine__6408__auto____18251$fn__18253.invoke(impl.clj:82)
;; 	at net.zopg.zo.session.impl$state_loop$fn__18234$state_machine__6408__auto____18251.invoke(impl.clj:82)
;; 	at clojure.core.async.impl.ioc_macros$run_state_machine.invokeStatic(ioc_macros.clj:973)
;; 	at clojure.core.async.impl.ioc_macros$run_state_machine.invoke(ioc_macros.clj:972)
;; 	at clojure.core.async.impl.ioc_macros$run_state_machine_wrapped.invokeStatic(ioc_macros.clj:977)
;; 	at clojure.core.async.impl.ioc_macros$run_state_machine_wrapped.invoke(ioc_macros.clj:975)
;; 	at clojure.core.async$ioc_alts_BANG_$fn__6637.invoke(async.clj:384)
;; 	at clojure.core.async$do_alts$fn__6569$fn__6572.invoke(async.clj:253)
;; 	at clojure.core.async.impl.channels.ManyToManyChannel$fn__605.invoke(channels.clj:135)
;; 	at clojure.lang.AFn.run(AFn.java:22)
;; 	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
;; 	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
;; 	at java.lang.Thread.run(Thread.java:745)

(defn select-1 []
  (let [client (zo/client conn-params)]
      (with-open [sess (<!! (zo/connect client))]
        (<!! (zo/q sess [:val "SELECT 1"])))))
