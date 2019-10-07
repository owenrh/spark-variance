package com.dataflow.spark

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class InfiniteStreamReceiver[T] (stream: Stream[T], delay: Long, numEvents: Int, storageLevel: StorageLevel) extends Receiver[T](storageLevel) {

  private val streamIterator = stream.iterator

  private class ReadAndStore extends Runnable {
    def run(): Unit = {
      while (streamIterator.hasNext) {
        for(_ <- 1 to numEvents) {
          val next = streamIterator.next()
          store(next)
        }

        try {
          Thread.sleep(delay)
        }
        catch {
          case _: InterruptedException => // ignore
        }
      }
    }
  }

  override def onStart(): Unit = {
    new Thread(new ReadAndStore).run()
  }

  override def onStop(): Unit = { }
}
