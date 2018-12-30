package io.github.pirocks.ipc

import io.github.pirocks.namedpipes.NamedPipe
import org.junit.Assert
import org.junit.Test

class SimpleIntegerTest{
    private val integerToSend = 6
    private val timesToSend = 10

    private val sendInt = Thread{
        val channel = NamedPipeChannel({},"int-test",false)
        repeat(timesToSend){
            channel.send(NamedPipeToSendMessage(integerToSend,channel))
        }

    }

    var timesReceived = 0

    private val receiveInt = Thread{
        val channel = NamedPipeChannel({
            Assert.assertTrue(it.contents is Int)
            Assert.assertEquals(it.contents,integerToSend)
            timesReceived++
        },"int-test",false)
        channel.close()
    }

    @Test(timeout = 100)//aggressive timeout to test performance
    fun testSendReceiveInt(){
        sendInt.start()
        receiveInt.start()
        sendInt.join()
        receiveInt.join()
        Assert.assertEquals(timesReceived,timesToSend)
    }
}

class RepliesTest{

}