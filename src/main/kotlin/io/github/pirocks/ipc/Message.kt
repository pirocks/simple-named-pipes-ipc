package io.github.pirocks.ipc

import io.github.pirocks.namedpipes.NamedPipe
import java.io.*
import java.lang.IllegalStateException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.experimental.and

interface Message<out Type> {
    val id : Int
    val contents: Type
}

interface ToSendMessage<MessageType> : Message<MessageType> {
    fun writeOut(dataOutputStream: DataOutputStream)
}

interface Reply<out MessageType> : ReceivedMessage<MessageType>

interface ReceivedMessage<out Type> : Message<Type> {
    fun reply(reply: Reply<*>)
}

interface Channel : Closeable {
    val onReceivedMessage: (ReceivedMessage<*>) -> Unit // not called for replies
    fun sendAwaitReply(message: ToSendMessage<*>): Reply<*>
    fun send(message: ToSendMessage<*>): Unit
    fun send(message: ToSendMessage<*>, onReply: (message: ReceivedMessage<*>) -> Unit): Unit
    override fun close()
}


open class MessageImpl<out Type>(override val contents: Type, override val id: Int) : ReceivedMessage<Type> {
    override fun reply(reply: Reply<*>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class ReplyImpl<out Type>(val message: ReceivedMessage<Type>, val replyToID: Int) /*: MessageImpl<Type>(message.contents, message.id),*/ :Reply<Type>{
    override val id: Int
        get() = message.id
    override val contents: Type
        get() = message.contents

    override fun reply(reply: Reply<*>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}

/**
 *
 */
class ChannelImpl(override val onReceivedMessage: (ReceivedMessage<*>) -> Unit, val name: String, val persist: Boolean = false, val channelHome: String = "/var/lib/java-simple-ipc") : Channel {
    companion object {

        //message types:
        const val SINGLE_MESSAGE: Byte = 1
        const val REPLY_MESSAGE: Byte = 2
        //todo batched message feature for sending multiple messages in one message to improve performance
        // content types,negative is an array of that type
        const val INT: Byte = 1
        const val BYTE: Byte = 2
        const val CHAR: Byte = 3
        const val UTF8: Byte = 4
        const val SHORT: Byte = 5
        const val FLOAT: Byte = 6
        const val DOUBLE: Byte = 7
        const val BOOLEAN: Byte = 8
        const val OBJECT: Byte = 9
        const val PROTOCOL_VERSION: Byte = 1

        const val CLOSE_TIMEOUT: Long = 10
    }
    private val sendPipe: NamedPipe

    private val receivePipe: NamedPipe
    private val readerThread: Thread
    private var continueReading = true
    private var messageIDCount = AtomicInteger(0)
    private val replies = mutableMapOf<Int, Reply<*>>()
    private val onReply = mutableMapOf<Int, (message: ReceivedMessage<*>) -> Unit>()
    private val waiting = mutableMapOf<Int, ReentrantLock>()
    init {
        (File(channelHome)).mkdirs()
        //todo validate names
        val sendName = name + "send"
        sendPipe = NamedPipe(File(channelHome, sendName), openExistingFile = true, deleteOnClose = !persist)
        val receiveName = name + "receive"
        receivePipe = NamedPipe(File(channelHome, receiveName), openExistingFile = true, deleteOnClose = !persist)
        readerThread = Thread {
            try {
                while (continueReading) {
                    readHandleMessage()
                }
            }catch (interrupted : InterruptedException){
                //todo log shutdown
            }
        }
    }

    private val receiveStream = receivePipe.readStream
    private val sendStream = sendPipe.writeStream

    private fun readHandleMessage(){
        val version = receiveStream.readByte()
        if (version != PROTOCOL_VERSION) {
            throw IllegalStateException("Wrong version")
        }
        val type = receiveStream.readByte()
        return when (type) {
            SINGLE_MESSAGE -> {
                handleMessage(readSingleMessage())
            }
            REPLY_MESSAGE -> {
                val replyToID = receiveStream.readInt()
                val replyMessage: ReceivedMessage<*> = readSingleMessage()
                val reply = ReplyImpl(replyMessage,replyToID)
                handleReply(reply,replyToID)
            }

            else -> throw IllegalStateException("Invalid data received")
        }
    }

    private fun readSingleMessage(): ReceivedMessage<*> {
        val contentsType = receiveStream.readByte()
        val messageID : Int = receiveStream.readInt()
        if (contentsType < 0) {
            val arrayLength= receiveStream.readInt()
            return MessageImpl((0 until arrayLength).map { readOfType(contentsType) }.toTypedArray(),messageID)
        }
        return MessageImpl(readOfType(contentsType),messageID)
    }

    private fun readOfType(contentsType: Byte): Any {
        return when (contentsType.and(0x7f)) {
            INT -> receiveStream.readInt()
            BYTE -> receiveStream.readByte()
            CHAR -> receiveStream.readChar()
            UTF8 -> receiveStream.readUTF()
            SHORT -> receiveStream.readShort()
            FLOAT -> receiveStream.readFloat()
            DOUBLE -> receiveStream.readDouble()
            BOOLEAN -> receiveStream.readBoolean()
            OBJECT -> ObjectInputStream(receiveStream).readObject()
            else -> {
                throw IllegalStateException("Invalid data recieved")
            }
        }
    }

    private fun handleReply(reply: Reply<*>, replyToID: Int) {
        if(replyToID in waiting){
            replies[replyToID] = reply
            while (!waiting[replyToID]!!.isLocked);//prevents unlocking before locking
            waiting[replyToID]!!.unlock()
        }else if (replyToID in onReply){
            onReply[replyToID]!!(reply)
        }else{
            throw IllegalStateException("Received a reply to a nonexistent message")
        }
    }

    private fun handleMessage(message: ReceivedMessage<*>) {
        onReceivedMessage.invoke(message)
    }

    private val sendLock = ReentrantLock()

    override fun sendAwaitReply(message: ToSendMessage<*>): Reply<*> {
        waiting[message.id] = ReentrantLock()
        send(message)
        waiting[message.id]!!.lock()
        waiting.remove(message.id)//don't leak locks
        val reply = replies[message.id]!!
        waiting.remove(message.id)//don't leak replies
        return reply
    }

    override fun send(message: ToSendMessage<*>) {
        sendLock.withLock {
            message.writeOut(sendStream)
        }
    }

    override fun send(message: ToSendMessage<*>, onReply: (message: ReceivedMessage<*>) -> Unit) {
        this.onReply[message.id] = onReply
        send(message)
    }

    override fun close() {
        continueReading = true
        try {
            readerThread.join(CLOSE_TIMEOUT)
        } catch (interruptedException: InterruptedException) {
        }
        readerThread.interrupt()
        sendPipe.close()
    }

    fun finalize() {
        close()
    }
}
