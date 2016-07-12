package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import org.zeromq.ZMQ;
//import org.zeromq.ZMsg;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import messaging.Message.Request;
import messaging.Message.Response;
/**
 * Hello world!
 *
 */
public class LatticedbClient extends DB {

  private static ZMQ.Context context = ZMQ.context(1);

  private static ThreadLocal<ZMQ.Socket> requester = new ThreadLocal<ZMQ.Socket>();

  private static ThreadLocal<HashMap<String, HashMap<Long, Long>>> versionMap = 
      new ThreadLocal<HashMap<String, HashMap<Long, Long>>>();

  // private static HashMap<String, HashMap<Long, Long>> versionMap = 
  //     new HashMap<String, HashMap<Long, Long>>();

  private static ThreadLocal<Request.Builder> req = new ThreadLocal<Request.Builder>();

  private static Random ran = new Random();

  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  public void init() throws DBException {
    //System.out.println(INIT_COUNT.incrementAndGet());
    //System.out.println("thread id is " + Thread.currentThread().getId());
    requester.set(context.socket(ZMQ.REQ));
    req.set(Request.newBuilder());
    versionMap.set(new HashMap<String, HashMap<Long, Long>>());
    requester.get().connect("tcp://localhost:5559");
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final HashMap<String, ByteIterator> result) {
    req.get().setType("GET");
    req.get().setKey(key);
    requester.get().send(req.get().build().toByteArray(), 0);
    req.get().clear();

    byte[] data = requester.get().recv(0);
    try {
      Response res = Response.parseFrom(data);
      versionMap.get().put(key, new HashMap<Long, Long>(res.getVersionVector()));
      //System.out.println("value is " + res.getValue());
    } catch (Exception e) {
      e.printStackTrace();
    }

    return Status.OK;
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    return Status.OK;
  }

  @Override
  public Status update(final String table, final String key, final HashMap<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  @Override
  public Status insert(final String table, final String key, final HashMap<String, ByteIterator> values) {

    req.get().setType("PUT");
    req.get().setKey(key);
    req.get().setValue(ran.nextInt(1000));

    //System.out.println("value to be put is " + req.getValue());

    if (!versionMap.get().containsKey(key)) {
      versionMap.get().put(key, new HashMap<Long, Long>());
      versionMap.get().get(key).put(Thread.currentThread().getId(), 
          Long.valueOf(1));
    } else if (versionMap.get().containsKey(key) && 
          !versionMap.get().get(key).containsKey(Thread.currentThread().getId())) {
      versionMap.get().get(key).put(Thread.currentThread().getId(), Long.valueOf(1));
    } else {
      versionMap.get().get(key).put(Thread.currentThread().getId(), 
          versionMap.get().get(key).get(Thread.currentThread().getId()) + 1);
    }

    for (Map.Entry<Long, Long> entry : versionMap.get().get(key).entrySet()) {
      req.get().getMutableVersionVector().put(entry.getKey(), entry.getValue());
    }

    requester.get().send(req.get().build().toByteArray(), 0);
    req.get().clear();

    byte[] data = requester.get().recv(0);
    try {
      Response res = Response.parseFrom(data);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return Status.OK;
  }

  @Override
  public Status delete(final String table, final String key) {
    return Status.OK;
  }

}
