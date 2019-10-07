package com.dataflow.spark

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class RandomIntStreamReceiver(numMin: Int, numRange: Int, streamInterval: Long, numEventsInInterval: Int, storageLevel: StorageLevel) extends Receiver[Int](storageLevel) {

  var running = false

  private class ReadAndStore extends Runnable {
    def run(): Unit = {
      while (running) {
        for(_ <- 1 to numEventsInInterval) {
          val random = scala.util.Random.nextInt(numRange)
          store(numMin + random)
        }

        try {
          Thread.sleep(streamInterval)
        }
        catch {
          case _: InterruptedException => // ignore
        }
      }
    }
  }

  override def onStart(): Unit = {
    running = true
    new Thread(new ReadAndStore).run()
  }

  override def onStop(): Unit = {
    running = false
  }
}
