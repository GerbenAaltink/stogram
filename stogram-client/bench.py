import os
import sys 

import asyncio

async def run_command(cmd):
    # Start a subprocess and wait for it to complete
    process = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    # Wait for the process to complete and get output and error
    stdout, stderr = await process.communicate()

    # Return the output, error, and exit code
    return process.returncode

async def main(cmd,times):
    tasks = [run_command(cmd )for x in range(times)]
    await asyncio.gather(*tasks)

times = int(sys.argv[1])

asyncio.run(main("./venv/bin/stogram", times))
