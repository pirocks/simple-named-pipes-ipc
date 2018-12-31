package io.github.pirocks.ipc

import org.junit.Assert
import org.junit.Test
import java.io.Serializable
import java.nio.file.Files
import java.util.concurrent.Semaphore
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class SimpleIntegerTest{
    private val integerToSend = 6
    private val timesToSend = 10
    private val channelHome = Files.createTempDirectory("ipc-test").toString()

    private val sendInt = Thread({
        val channel = NamedPipeChannel({},"int-test",false,channelHome = channelHome)
        repeat(timesToSend){
            channel.send(NamedPipeToSendMessage(integerToSend,channel))
        }
        channel.close()
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
    data class TypeToSend(val i : Int, val c: Char): Serializable
    private val toSend : TypeToSend = TypeToSend(9089,'c')
    private val toReply : TypeToSend = TypeToSend(8970,'a')
    private val channelHome = Files.createTempDirectory("ipc-test").toString()
    private var numReplies = 0

    private val send = Thread({
        val channel = NamedPipeChannel({},"reply-test",false,channelHome)
        val sendAwaitReply = channel.sendAwaitReply(NamedPipeToSendMessage(toSend, channel))
        Assert.assertEquals(sendAwaitReply.contents,toReply)
        val awaitReplySema = Semaphore(0)
        var received = false
        channel.send(NamedPipeToSendMessage(toSend,channel)) {
            received = true
            Assert.assertEquals(sendAwaitReply.contents,toReply)
            awaitReplySema.release()
        }
        awaitReplySema.acquire()
        Assert.assertTrue(received)

    },"send")

    private val reply = Thread({
        val channel = NamedPipeChannel({
            numReplies++
            Assert.assertEquals(it.contents,toSend)
            it.reply(NamedPipeToSendMessage(toReply,it.channel))
        },"reply-test",false,channelHome)
    },"reply")

    @Test
    fun testReply(){
        send.start()
        reply.start()
        send.join()
        reply.join()
        Assert.assertEquals(numReplies,2)
    }
}