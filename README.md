# Scrutiny Server in Rust

This is a Rust implementation of a Scrutiny Server.  
Scrutiny is a GUI tool for monitoring and analyzing data from various sources.  
The server is used to collect data from individual sources and provides an common API for the Scrutiny GUI to visualize and analyze the collected data.  

For more information about Scrutiny, please visit the official Scrutiny GitHub repository:
https://github.com/scrutinydebugger/scrutiny-main



Run the server example application sine_source:

cargo run --example sine_source


Run the Scrutiny GUI:

scrutiny gui    --auto-connect


