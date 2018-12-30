package io.github.pirocks.ipc

import io.github.pirocks.namedpipes.NamedPipe
import org.junit.Assert
import org.junit.Test
import java.nio.file.Files

class SimpleIntegerTest{
    private val integerToSend = 6
    private val timesToSend = 10
    private val channelHome = Files.createTempDirectory("ipc-test").toString()

    private val sendInt = Thread({
        val channel = NamedPipeChannel({},"int-test",false,channelHome = channelHome)
        repeat(timesToSend){
            channel.send(NamedPipeToSendMessage(integerToSend,channel))
        }
    },"sendInt")

    var timesReceived = 0

    private val receiveInt = Thread({
        val channel = NamedPipeChannel({
            Assert.assertTrue(it.contents is Int)
            Assert.assertEquals(integerToSend,it.contents)
            timesReceived++
        },"int-test",false,channelHome = channelHome)
        channel.close()
    },"recieveInt")

//    @Test(timeout = 100)//aggressive timeout to test performance
    @Test
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