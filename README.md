# SeamlessDB

SeamlessDB is a prototype cloud database system for academic purposes, supporting the separation of computation, state, and storage into three layers.

We have implemented all the designs mentioned in the paper(SeamlessDB: Efficient Instance Transition in Cloud-native Databases with Breakpoint Resumption) based on this system.


## How to build
```bash
# build storage pool, state pool, rw server, ro server, proxy
bash build.sh
# build seamless client
cd seamless_client
mkdir build && cd build
cmake .. && cmake --build .
```

## How to run SeamlessDB
Before running the program, you need to fill in the configuration file first. All configuration files are in [here](./src/config/) and we provide a default configuration.
```bash
cd build
# launch stoarge pool
./bin/storage_pool
# launch state_pool
./bin/state_pool
# launch rw_server
./bin/rw_server active
# launch client
cd seamless_client
./build/bin/seamless_client
# launch proxy(be sure that storage pool, state pool and rw server have been launched)
./bin/proxy
```

## How to run experiments
We use scripts to run experiments. Before run experiments, you have to make sure that the storage pool, state pool and rw server have been launched.

```bash
./bin/storage_pool
./bin/state_pool
./bin/rw_server active
./bin/rw_server backup
cd scripts
bash run_test 40
```

run experiments in batch
```bash
python run_theta_block.py
```
