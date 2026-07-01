StarPy Asterisk Protocols for asyncio
=====================================

StarPy is a Python asyncio library that provides access to the Asterisk
PBX's Manager Interface (AMI) and Fast Asterisk Gateway Interface (FastAGI).
Together these allow you write both command-and-control interfaces (used, for
example to generate new calls) and to customise user interactions from the
dialplan. You can readily write applications that use the AMI and FastAGI
protocol together with any of the already available asyncio protocols.

StarPy is primarily intended to allow asyncio developers to add Asterisk
connectivity to their asyncio applications. It isn't really targeted at the
normal AGI-writing populace, as it requires understanding asyncio's
asynchronous programming model. That said, if you do know asyncio, it can
readily be used to write stand-alone FastAGIs.

StarPy is Open Source and we are interested in contributions, bug reports and
feedback.
