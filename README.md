# simple-named-pipes-ipc
A (hopefully) simple library for interprocess communication between 2 processes in java. Processes can send messages containing primitive types, arrays, or serializable objects, on various channels. Processes can reply to messages and await replies. As the name suggests this library uses named pipes to transfer data to/from processes. 


```xml
<dependency>
  <groupId>io.github.pirocks</groupId>
  <artifactId>named-pipes-ipc-lib</artifactId>
  <version>0.0.2</version>
</dependency>
```
