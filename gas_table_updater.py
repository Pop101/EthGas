import gas_table
import time

block = 0
timestamp = time.time()
while True:
    gas_table.update()

    new_block = gas_table.get_recent_gas()[1][0][1]
    if block < new_block:
        print(f'Block is now {new_block} (lasted {round(time.time()-timestamp,2)}s)')
        block = new_block
        timestamp = time.time()

    time.sleep(1) # Ratelimit by etherscan is 0.2 (1/5)th 