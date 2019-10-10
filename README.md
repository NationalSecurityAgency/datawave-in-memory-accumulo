# In-Memory Accumulo

In-Memory Accumulo is a simple in-memory implementation of Accumulo.
This does not attempt to replicate all features of Accumulo but rather
enough to use for executing unit tests that need to interact with
Accumulo (where starting Mini Accumulo Cluster might take too long) or
for use as an in-memory local cache fronting a remote Accumulo server.