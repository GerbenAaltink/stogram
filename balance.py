import asyncio 
import random 

async def connect(host, port):
    reader, writer = await asyncio.open_connection(host, port)
    return reader,writer 
async def stream(reader, writer,port):
    while True:
        chunk = await reader.read(4096)
        
        if chunk and b'Index' in chunk:
            chunk = chunk.replace(b'Index',str(port).encode('utf-8'))
        if not chunk: 
            writer.close()
            return 
        writer.write(chunk)
        await writer.drain()

def get_random_port():
    return random.choice([8084,8085,8086])

async def forward(reader, writer, port): 
    print("Streaming to port",port)
    upstream_reader, upstream_writer = await connect("127.0.0.1", port)
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
    tasks.append(asyncio.create_task( serve("127.0.0.1", 7000,[8084,8085,8086])))
    tasks.append(asyncio.create_task(serve("127.0.0.1", 7001,[8889])))
    await asyncio.gather(*tasks)

asyncio.run(main())