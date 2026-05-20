# Scrutiny Server in Rust

This is a Rust implementation of a simple RPV (Runtime Published Values) Scrutiny Server.  

Scrutiny is a GUI tool for tuning and observing data on a target system.  
The server collects data from individual sources and serves the API to the Scrutiny GUI, which visualizes the collected data. The server also allows to modify data on the target system, which is useful for tuning and testing purposes. 

For more information about Scrutiny, please visit the official Scrutiny GitHub repository:
https://github.com/scrutinydebugger/scrutiny-main

and

https://scrutinydebugger.com



Run the server example application sine_source:

cargo run --example sine_source


Run the Scrutiny GUI:

scrutiny gui    --auto-connect


