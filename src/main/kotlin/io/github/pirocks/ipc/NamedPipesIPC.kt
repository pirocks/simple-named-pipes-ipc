package io.github.pirocks.ipc

import com.sun.security.auth.module.UnixSystem
import io.github.pirocks.namedpipes.NamedPipe
import java.lang.IllegalArgumentException
import java.lang.IllegalStateException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.experimental.and
import java.io.*
import java.nio.channels.FileLock


open class NamedPipeToSendMessage<Type>(override val contents: Type, override val channel: NamedPipeChannel) :
        ToSendMessage<Type,NamedPipeChannel> {

    override fun writeOut(dataOutputStream: DataOutputStream) {
        writeMessageType(dataOutputStream)
        val contentsCopy = contents//needed for smart casting
        if (contentsCopy is Array<*>) {
            val len = contentsCopy.size
            val firstItem = contentsCopy[0]
            dataOutputStream.writeByte(anyToByteEncodingType(firstItem!!))//todo handle null items
            dataOutputStream.writeInt(len)
            contentsCopy.forEach { writeAny(it!!,dataOutputStream) }
        } else if (anyToByteEncodingType(contentsCopy!!) < 0) {
            val len = when(contentsCopy) {
                is IntArray -> contentsCopy.size
                is ByteArray -> contentsCopy.size
                is CharArray -> contentsCopy.size
                is ShortArray -> contentsCopy.size
                is FloatArray -> contentsCopy.size
                is DoubleArray -> contentsCopy.size
                is BooleanArray -> contentsCopy.size
                else -> throw IllegalStateException("This should not happen")
            }
            dataOutputStream.writeByte(anyToByteEncodingType(contentsCopy))
            dataOutputStream.writeInt(len)
            when(contentsCopy) {
                is IntArray -> contentsCopy.forEach(dataOutputStream::writeInt)
                is ByteArray -> contentsCopy.forEach { dataOutputStream.writeByte(it.toInt()) }
                is CharArray -> contentsCopy.forEach { dataOutputStream.writeChar(it.toInt()) }
                is ShortArray -> contentsCopy.forEach { dataOutputStream.writeShort(it.toInt()) }
                is FloatArray -> contentsCopy.forEach(dataOutputStream::writeFloat)
                is DoubleArray -> contentsCopy.forEach(dataOutputStream::writeDouble)
                is BooleanArray -> contentsCopy.forEach(dataOutputStream::writeBoolean)
                else -> throw IllegalStateException("This should not happen")
            }
        } else {
            dataOutputStream.writeByte(anyToByteEncodingType(contentsCopy))
            when (contentsCopy) {
                is Int -> dataOutputStream.writeInt(contentsCopy)
                is Byte -> dataOutputStream.writeByte(contentsCopy.toInt())
                is Char -> dataOutputStream.writeChar(contentsCopy.toInt())
                is String -> dataOutputStream.writeUTF(contentsCopy)
                is Short -> dataOutputStream.writeShort(contentsCopy.toInt())
                is Float -> dataOutputStream.writeFloat(contentsCopy)
                is Double -> dataOutputStream.writeDouble(contentsCopy)
                is Boolean -> dataOutputStream.writeBoolean(contentsCopy)
                else -> ObjectOutputStream(dataOutputStream).writeObject(contentsCopy)
            }
        }
    }

    protected open fun writeMessageType(dataOutputStream: DataOutputStream) {
        dataOutputStream.writeByte(NamedPipeChannel.SINGLE_MESSAGE.toInt())
    }

    private fun writeAny(any: Any, dataOutputStream: DataOutputStream) {
        when (any) {
            is Int -> dataOutputStream.writeInt(any)
            is Byte -> dataOutputStream.writeByte(any.toInt())
            is Char -> dataOutputStream.writeChar(any.toInt())
            is String -> dataOutputStream.writeUTF(any)
            is Short -> dataOutputStream.writeShort(any.toInt())
            is Float -> dataOutputStream.writeFloat(any)
            is Double -> dataOutputStream.writeDouble(any)
            is Boolean -> dataOutputStream.writeBoolean(any)
            else -> ObjectOutputStream(dataOutputStream).writeObject(any)
        }
    }

    private fun anyToByteEncodingType(any: Any): Int {
        return when (any) {
            is Int -> +NamedPipeChannel.INT
            is Byte -> +NamedPipeChannel.BYTE
            is Char -> +NamedPipeChannel.CHAR
            is String -> +NamedPipeChannel.UTF8
            is Short -> +NamedPipeChannel.SHORT
            is Float -> +NamedPipeChannel.FLOAT
            is Double -> +NamedPipeChannel.DOUBLE
            is Boolean -> +NamedPipeChannel.BOOLEAN
            is IntArray -> -NamedPipeChannel.INT
            is ByteArray -> -NamedPipeChannel.BYTE
            is CharArray -> -NamedPipeChannel.CHAR
            is ShortArray -> -NamedPipeChannel.SHORT
            is FloatArray -> -NamedPipeChannel.FLOAT
            is DoubleArray -> -NamedPipeChannel.DOUBLE
            is BooleanArray -> -NamedPipeChannel.BOOLEAN
            else -> +NamedPipeChannel.OBJECT
        }
    }
}

class NamedPipeToSendReply<Type>(override val contents: Type, val replyToID: Int, channel: NamedPipeChannel) :
        NamedPipeToSendMessage<Type>(contents,channel), ToSendMessage<Type,NamedPipeChannel> {
    override fun writeMessageType(dataOutputStream: DataOutputStream) {
        dataOutputStream.writeByte(NamedPipeChannel.REPLY_MESSAGE.toInt())
        dataOutputStream.writeInt(replyToID)
    }

}

open class NamedPipeReceivedMessage<Type>(override val contents: Type, val id: Int, override val channel: NamedPipeChannel) :
        ReceivedMessage<Type,NamedPipeChannel> {
    override fun <ReplyType> reply(reply: ToSendMessage<ReplyType, NamedPipeChannel>) {
        channel.send(NamedPipeToSendReply(reply.contents,this.id,channel))
    }

}

class NamedPipeReceivedReply<Type>(val message: NamedPipeReceivedMessage<Type>, channel: NamedPipeChannel) :
        NamedPipeReceivedMessage<Type>(message.contents,message.id,channel) , Reply<Type,NamedPipeChannel> {
    override val contents
        get() = message.contents

}

private val filesystemLockProtector = ReentrantLock()// allows for filesystem locks to be used safely from the same jvm


/**
 *  message specification:
 *  protocol version (byte)
 *  message id (int)
 *  message type (byte)
 *  for single message types:
 *  content type (negative for array) (byte)
 *  array length (if applicable) (int)
 *  message contents
 *  for reply message types:
 *  id of message replying to
 *  single message containing reply
 */
class NamedPipeChannel(override val onReceivedMessage: (ReceivedMessage<*,NamedPipeChannel>) -> Unit, val name: String, val persist: Boolean = false, val channelHome: String = "/var/run/user/" + UnixSystem().uid + "/simple-java-ipc-named-pipes-lib/") : Channel<NamedPipeChannel> {
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

    private lateinit var sendPipe: NamedPipe

    private lateinit var receivePipe: NamedPipe
    private val readerThread: Thread
    private var continueReading = true
    internal var messageIDCount = AtomicInteger(0)
    private val replies = ConcurrentHashMap<Int, Reply<*,NamedPipeChannel>>()
    private val onReply = ConcurrentHashMap<Int, (message: ReceivedMessage<*,NamedPipeChannel>) -> Unit>()
    private val waiting = ConcurrentHashMap<Int, ReentrantLock>()

    private val sendStream : DataOutputStream
    private lateinit var receiveStream : DataInputStream

    init {
        val channelHomeCreated = (File(channelHome)).mkdir()
        if (!channelHomeCreated && !File(channelHome).exists()){
            throw IllegalArgumentException("Unable to create channel home directory")
        }
        openPipes()
        readerThread = Thread( {
            try {
                receiveStream = receivePipe.readStream
                while (continueReading) {
                    readHandleMessage()
                }
            } catch (interrupted: InterruptedException) {
                //todo log shutdown
            }
        },"NamedPipeReader")
        readerThread.start()
        sendStream = sendPipe.writeStream
    }

    private fun openPipes() {
        filesystemLockProtector.withLock {
            val fileLock = acquireFileSystemLock()
            try {
                sendPipe = NamedPipe(File(channelHome, name + "0"), openExistingFile = false,overWriteExistingFile = false, deleteOnClose = !persist)
                receivePipe = NamedPipe(File(channelHome, name + "1"),openExistingFile = false,overWriteExistingFile = false, deleteOnClose = !persist)
            }catch (e:NamedPipe.Companion.FileAlreadyExists){
                sendPipe = NamedPipe(File(channelHome, name + "1"), openExistingFile = true,overWriteExistingFile = false, deleteOnClose = !persist)
                receivePipe = NamedPipe(File(channelHome, name + "0"),openExistingFile = true,overWriteExistingFile = false, deleteOnClose = !persist)
            }
            releaseFileSystemLock(fileLock)
        }
    }

    private fun acquireFileSystemLock(): FileLock {
        val lockFile = File(channelHome, "$name.lock")
        return RandomAccessFile(lockFile,"rw").channel.lock()
    }

    private fun releaseFileSystemLock(fileLock: FileLock){
        fileLock.release()
    }

    private fun readHandleMessage() {
        val version = receiveStream.readByte()
        if (version != PROTOCOL_VERSION) {
            throw IllegalStateException("Wrong version")
        }
        val id = receiveStream.readInt()
        val type = receiveStream.readByte()
        return when (type) {
            SINGLE_MESSAGE -> {
                handleMessage(readSingleMessage(id))
            }
            REPLY_MESSAGE -> {
                val replyToID = receiveStream.readInt()
                val replyMessage: NamedPipeReceivedMessage<*> = readSingleMessage(id)
                val reply = NamedPipeReceivedReply(replyMessage, this)
                handleReply(reply, replyToID)
            }

            else -> throw IllegalStateException("Invalid data received")
        }
    }

    private fun readSingleMessage(id : Int): NamedPipeReceivedMessage<*> {
        val contentsType = receiveStream.readByte()
        if (contentsType < 0) {
            val arrayLength = receiveStream.readInt()
            return NamedPipeReceivedMessage((0 until arrayLength).map { readOfType(contentsType) }.toTypedArray(),id,this)
        }
        return NamedPipeReceivedMessage(readOfType(contentsType),id,this)
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
                throw IllegalStateException("Invalid data received")
            }
        }
    }

    private fun handleReply(reply: Reply<*,NamedPipeChannel>, replyToID: Int) {
        when (replyToID) {
            in waiting -> {
                replies[replyToID] = reply
                while (!waiting[replyToID]!!.isLocked);//prevents unlocking before locking
                waiting[replyToID]!!.unlock()
            }
            in onReply -> onReply[replyToID]!!(reply)
            else -> throw IllegalStateException("Received a reply to a nonexistent message")
        }
    }

    private fun handleMessage(message: ReceivedMessage<*,NamedPipeChannel>) {
        onReceivedMessage.invoke(message)
    }

    private val sendLock = ReentrantLock()

    private fun writePreamble(id : Int){
        assert(sendLock.isHeldByCurrentThread)
        sendStream.writeByte(PROTOCOL_VERSION.toInt())
        sendStream.writeInt(id)
    }

    private fun sendImpl(message: ToSendMessage<*, NamedPipeChannel>, id : Int){
        sendLock.withLock {
            writePreamble(id)
            message.writeOut(sendStream)
        }
    }

    override fun send(message: ToSendMessage<*,NamedPipeChannel>) {
        val id = messageIDCount.getAndIncrement()
        sendImpl(message, id)
    }

    override fun sendAwaitReply(message: ToSendMessage<*,NamedPipeChannel>): Reply<*,NamedPipeChannel> {
        val messageId = messageIDCount.getAndIncrement()
        waiting[messageId] = ReentrantLock()
        send(message)
        waiting[messageId]!!.lock()
        waiting.remove(messageId)//don't leak locks
        val reply = replies[messageId]!!
        waiting.remove(messageId)//don't leak replies
        return reply
    }

    override fun send(message: ToSendMessage<*,NamedPipeChannel>, onReply: (message: ReceivedMessage<*,NamedPipeChannel>) -> Unit) {
        val messageId = messageIDCount.getAndIncrement()
        this.onReply[messageId] = onReply
        send(message)
    }

    override fun close() {
        continueReading = true
        try {
            readerThread.join(CLOSE_TIMEOUT)
        } catch (interruptedException: InterruptedException) {}
        readerThread.interrupt()
        sendPipe.close()
    }

    fun finalize() {
        close()
    }
}
