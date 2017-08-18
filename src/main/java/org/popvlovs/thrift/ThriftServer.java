package org.popvlovs.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Created by Think on 2017/8/16.
 *
 * @author popvlovs
 */

public class ThriftServer {

    private final static InetSocketAddress INET = new InetSocketAddress("192.168.199.132", 9091);

    /**
     * 1、使用TNonblockingServerSocket必须配合TFramedTransport使用
     * 2、最好与TThreadedSelectorServer配合使用
     *
     * @param args ...
     */
    public static void main(String[] args) {
        initNonBlockingServer();
    }

    private static class TestServiceImpl implements TestService.AsyncIface {

        @Override
        public void subscribe(String id, AsyncMethodCallback<String> resultHandler) throws TException {
            System.out.println("收到调用请求subscribe，正在写入返回值：" + id);
            resultHandler.onComplete(id);
        }

        @Override
        public void publish(String id, Map<String, String> params, AsyncMethodCallback<Void> resultHandler) throws TException {

            // 处理
            System.out.println("收到调用请求publish");
        }
    }

    private static void initNonBlockingServer() {
        try {
            // 处理层(Processor)
            TestServiceImpl handler = new TestServiceImpl();
            TestService.AsyncProcessor<TestServiceImpl> processor = new TestService.AsyncProcessor(handler);

            // 服务层(Server)
            TServer server = new TThreadedSelectorServer(
                    new TThreadedSelectorServer.Args(new TNonblockingServerSocket(INET))            // 传输层(Server Transport)
                            .transportFactory(new TFramedTransport.Factory())
                            .protocolFactory(new TCompactProtocol.Factory())     // 协议层(Protocol)
                            .processorFactory(new TProcessorFactory(processor))
            );


            System.out.println("Starting the non-blocking server...");
            server.serve();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


