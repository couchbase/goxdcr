# Vagrant file set up

**Vagrantfile_6**: the vagrant file that should be used for 6.6.x versions.
**Vagrantfile_7**: the vagrant file that should be used for 7.x versions
**Vagrantfile**: should be a soft-link that points to one of the above depending on what is being tested

The reason there is a need for this is because Couchbase Server 6.6.x would be running on Ubuntu 18.04
and for 7.x will be running on Ubuntu 20.04... and different VM images are needed

When running for the first time, .vagrant is going to be created for you

Whenever switching between a vagrant file, the directory ".vagrant" should be removed so that new VMs are created
You can think of ".vagrant" as the artifact that is related to a specific Vagrantfile that was used
If needed, you can also save the .vagrant somewhere else for preservation and softlink them as needed

In other words, .vagrant + Vagrantfile are a pair and can be swapped out as needed

# Running

First, ensure that Vagrant file is soft linked correctly.

    $ rm Vagrantfile; ln -s Vagrantfile_7 Vagrantfile

If a clean state is needed, remove the `.vagrant` directory.

    $ rm -rf .vagrant

Run the provision script to start

    $ provision_clusterRun_vagrant.sh

# Network Fault Injection

The `vagrantInjectNetworkFault` function allows you to inject network packet loss on specific nodes to simulate network failures and test XDCR behavior under adverse conditions.

## Usage

```bash
vagrantInjectNetworkFault <clusterName>
```

Where `<clusterName>` is the cluster identifier from `VAGRANT_VM_IP_MAP` (e.g., `C1`, `C1P`, `C2`, `C2P`).

The function uses `getClusterIdx` to map the cluster name to the corresponding node index and then applies network faults to all network interfaces on that node.

## How it works

1. The function uses the `--seed` argument (via `RANDOM_SEED_ARG` environment variable) to deterministically generate a network fault percentage (0-8%)
2. Maps the cluster name (e.g., "C1") to a node index using `getClusterIdx` 
3. Installs `net-tools` and `iproute2` on the target VM if not already present
4. **Automatically parses IP addresses** from the Vagrantfile for the target node (no hardcoded IPs!)
5. For each IP address configured on the node:
   - Discovers the network interface using `ifconfig`
   - Applies traffic control (tc) rules to inject packet loss using `netem`
6. Example: `sudo tc qdisc add dev eth0 root netem loss 5%`

**Note**: If the Vagrantfile cannot be parsed, the function falls back to querying the VM directly using `hostname -I`.

## Cluster to Node Mapping

Based on the typical test case configuration:
- `C1` → node1 (192.168.56.2, 192.168.56.6)
- `C1P` → node2 (192.168.56.3, 192.168.56.7)
- `C2` → node3 (192.168.56.4, 192.168.56.8)
- `C2P` → node4 (192.168.56.5, 192.168.56.9)

## Example

```bash
# Set up cluster mapping (usually done in test case files)
CLUSTER_NAME_PORT_MAP=(["C1"]=15000 ["C1P"]=15001 ["C2"]=15002 ["C2P"]=15003)
VAGRANT_VM_IP_MAP=(["C1"]="192.168.56.2" ["C1P"]="192.168.56.3" ["C2"]="192.168.56.4" ["C2P"]="192.168.56.5")

# Set a seed to get deterministic behavior
export RANDOM_SEED_ARG="0x1234"

# Inject network faults on C1 (node1)
vagrantInjectNetworkFault "C1"

# Inject network faults on C2 (node3)
vagrantInjectNetworkFault "C2"
```

## Testing

A test script is provided to demonstrate usage:

```bash
./testNetworkFaultInjection.sh --seed 0x1234
```
