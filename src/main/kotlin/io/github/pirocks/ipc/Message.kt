package io.github.pirocks.ipc

import java.io.Closeable
import java.io.DataOutputStream

interface Message<Type, ChannelType: Channel<ChannelType>> {
    val contents: Type
    val channel: ChannelType
}

interface ToSendMessage<MessageType, ChannelType: Channel<ChannelType>> : Message<MessageType, ChannelType> {
    fun writeOut(dataOutputStream: DataOutputStream)
}

interface Reply<MessageType,ChannelType: Channel<ChannelType>> : ReceivedMessage<MessageType,ChannelType>

interface ReceivedMessage<Type,ChannelType: Channel<ChannelType>> : Message<Type,ChannelType>

interface Channel<ChannelType: Channel<ChannelType>> : Closeable {
    val onReceivedMessage: (ReceivedMessage<*,ChannelType>) -> Unit // not called for replies
    fun sendAwaitReply(message: ToSendMessage<*,ChannelType>): Reply<*,ChannelType>
    fun send(message: ToSendMessage<*,ChannelType>)
    fun send(message: ToSendMessage<*,ChannelType>, onReply: (message: ReceivedMessage<*,ChannelType>) -> Unit)
    override fun close()
}

