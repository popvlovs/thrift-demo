package org.popvlovs.thrift;

import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TNonblockingSocket;

import java.net.InetSocketAddress;

/**
 * Created by Think on 2017/8/16.
 *
 * @Author: popvlovs
 */
public class ThriftClient {

    private final static InetSocketAddress INET = new InetSocketAddress("192.168.199.132", 9091);

    public static void main(String[] args) {
        try {
            // 远程调用
            TestService.AsyncClient asyncClient = new TestService.AsyncClient(new TCompactProtocol.Factory(),
                    new TAsyncClientManager(),
                    new TNonblockingSocket(INET.getHostName(), INET.getPort()));

            System.out.println("Client calls .....");
            asyncClient.subscribe("popvlovs", new AsyncMethodCallback<String>() {
                @Override
                public void onComplete(String response) {
                    System.out.println("RPC返回值：" + response);
                }

                @Override
                public void onError(Exception exception) {
                    exception.printStackTrace();
                }
            });

            while (true) {
                try {
                    Thread.sleep(1);
                } catch (Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
