#!/usr/bin/env python3
import asyncio 
import random 

async def connect(host, port):
    reader, writer = await asyncio.open_connection(host, port)
    return reader,writer 
async def stream(reader, writer,port):
    while True:
        chunk = await reader.read(4096)
        
        if chunk and b'Index' in chunk:
            chunk = chunk.replace(b'Index',b'p' + str(port).encode('utf-8'))
        if not chunk: 
            writer.close()
            return 
        writer.write(chunk)

async def forward(reader, writer, port): 
    print("Streaming to port",port)
    upstream_reader, upstream_writer = await connect("stogram", port)
    tasks = []
    tasks.append(asyncio.create_task(stream(reader, upstream_writer,port)))
    tasks.append(asyncio.create_task(stream(upstream_reader, writer,port)))
    await asyncio.gather(*tasks)



async def serve(host, port, ports):

    async def handle_client(reader, writer):
        upstream_port = random.choice(ports)
        await forward(reader, writer, upstream_port)

    server = await asyncio.start_server(handle_client, host, port)
    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')

    async with server:
        await server.serve_forever()

async def main():
    tasks = []
    tasks.append(asyncio.create_task( serve("0.0.0.0", 7000,[8086,8085,8084])))
    tasks.append(asyncio.create_task(serve("0.0.0.0", 7001,[8889])))
    await asyncio.gather(*tasks)

asyncio.run(main())
