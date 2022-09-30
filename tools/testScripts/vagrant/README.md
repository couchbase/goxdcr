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
