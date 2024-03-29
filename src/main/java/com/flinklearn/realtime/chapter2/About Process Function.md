A **Process Function** is a low-level processing function.
Operators like Map and Filter are high-level functions that provide out-of-the-box functionality but limited flexibility.
On the other hand, a process function provides more flexibility and access to more resources, but needs more coding effort. It provides access to Events, State management elements, Timers and Side outputs. It helps to add custom controls in stream processing.

Flink has the concept of **Runtime Context**, which keeps track of the processing pipeline's active elements, including Accumulators, Broadcast variables, Cache, Configuration, and State. A process function provides access to the runtime context.

Typically, there is only one main data stream that is emitted by most operators. A process function can emit an additional data stream as a **Side Output**.
You can use side output to split data streams horizontally or vertically. One half of the output can go to the main output, and the other can go into the side output.
A given input record can emit either one of the outputs or both. Side output can be used to emit the same record in different formats. It can also be used to generate supplementary streams for logging and auditing.