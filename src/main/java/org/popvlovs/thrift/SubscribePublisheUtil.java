package org.popvlovs.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Think on 2017/8/16.
 */
public class SubscribePublisheUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubscribePublisheUtil.class.getName());

    private static SubscribePublishService.AsyncClient client = null;

    public static boolean publish(String topic, Map<String, String> data) {

        try {
            getClient().publish(topic, data, new AsyncMethodCallback<Void>() {
                @Override
                public void onComplete(Void response) {
                    LOGGER.debug("发布信息成功");
                }

                @Override
                public void onError(Exception exception) {
                    LOGGER.debug("发布信息失败");
                }
            });
        } catch (TException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public static boolean subscribe(String topic, AsyncMethodCallback<String> resultHandler) {

        // 注册本地服务，用于响应server分发过来的publish请求
        runClientServer(resultHandler);

        // 同时将topic和身份签名信息注册到订阅/分发服务器上
        InetSocketAddress address = getLocalAddress();
        IdentifyCard id = new IdentifyCard(address.getHostName(), address.getPort());

        try {
            getClient().subscribe(id, topic, new AsyncMethodCallback<String>() {
                @Override
                public void onComplete(String response) {
                    LOGGER.debug("成功注册topic至订阅/分发服务器");
                }

                @Override
                public void onError(Exception exception) {
                    LOGGER.error("注册失败：client.subscribe");
                }
            });
        } catch (TException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public static void runServer() {

        SubscribePublisheUtil.SubscribePublishServerImpl handler = new SubscribePublisheUtil.SubscribePublishServerImpl();
        serveWithThread(handler);
    }

    private static void serveWithThread(SubscribePublishService.AsyncIface handler) {

        new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    // 处理层(Processor)
                    SubscribePublishService.AsyncProcessor processor = new SubscribePublishService.AsyncProcessor(handler);

                    // 服务层(Server)
                    TServer server = new TThreadedSelectorServer(
                            new TThreadedSelectorServer.Args(new TNonblockingServerSocket(getLocalAddress()))            // 传输层(Server Transport)
                                    .transportFactory(new TFramedTransport.Factory())
                                    .protocolFactory(new TCompactProtocol.Factory())     // 协议层(Protocol)
                                    .processorFactory(new TProcessorFactory(processor))
                    );

                    // 启动本地服务
                    server.serve();

                } catch (TTransportException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    /**
     * 用于接收服务端传回的publish请求
     */
    private static void runClientServer(AsyncMethodCallback<String> resultHandler) {

        SubscribePublisheUtil.ClientHandler handler = new SubscribePublisheUtil.ClientHandler(resultHandler);
        serveWithThread(handler);
    }

    private static InetSocketAddress getLocalAddress() {

        // 暂时写死，应该是从配置文件里读，或者自动检测才对
        return new InetSocketAddress("192.168.199.132", 9092);
    }

    private static InetSocketAddress getServerAddress() {

        // 暂时写死，应该是从配置文件里读，或者自动检测才对
        return new InetSocketAddress("192.168.199.132", 9091);
    }

    private static SubscribePublishService.AsyncClient getClient() {
        try {
            if (client == null) {
                InetSocketAddress localAddr = getLocalAddress();
                client = new SubscribePublishService.AsyncClient(new TCompactProtocol.Factory(),
                        new TAsyncClientManager(),
                        new TNonblockingSocket(localAddr.getHostName(), localAddr.getPort()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return client;
    }

    private static class SubscribePublishServerImpl implements SubscribePublishService.AsyncIface {

        private static Map<IdentifyCard, String> topicMap = new ConcurrentHashMap();

        @Override
        public void subscribe(IdentifyCard id, String topic, AsyncMethodCallback<String> resultHandler) throws TException {

            InetSocketAddress address = new InetSocketAddress(id.getIp(), id.getPort());
            System.out.println("收到调用请求subscribe，topic:" + topic + ", from: " + address.toString());

            // 将id存入map
            topicMap.put(id, topic);

            resultHandler.onComplete(topic);
        }

        @Override
        public void publish(String id, Map<String, String> params, AsyncMethodCallback<Void> resultHandler) throws TException {

            // 处理
            System.out.println("收到调用请求publish");
        }
    }

    private static class ClientHandler implements SubscribePublishService.AsyncIface {

        private AsyncMethodCallback<String> handler;

        public ClientHandler(AsyncMethodCallback<String> handler) {
            this.handler = handler;
        }

        @Override
        public void subscribe(IdentifyCard id, String topic, AsyncMethodCallback<String> resultHandler) throws TException {

        }

        @Override
        public void publish(String id, Map<String, String> params, AsyncMethodCallback<Void> resultHandler) throws TException {

            // 不应该被调用 v 0.1
            assert false;
        }
    }
}
