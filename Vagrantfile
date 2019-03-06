Vagrant.configure("2") do |config|

  config.vm.define "test" do |test|
    test.vm.box = "centos/7"
    test.vm.provider "virtualbox" do |vb|
        vb.memory = 2048
    end

    test.vm.network "private_network", ip: "192.168.250.10"
    test.vm.hostname = "test.cassandra-backup-service.local"

    # Disable shared folders between host and guest.
    test.vm.synced_folder '.', '/vagrant', disabled: true

    test.vm.provision "shell", inline: <<-SHELL
      sudo -s
      echo -e "[cassandra]\nname=Apache Cassandra\nbaseurl=https://www.apache.org/dist/cassandra/redhat/22x/\ngpgcheck=1\nrepo_gpgcheck=1\ngpgkey=https://www.apache.org/dist/cassandra/KEYS\n" > /etc/yum.repos.d/cassandra.repo
      yum -y install cassandra
      service cassandra start
      curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
      python get-pip.py
      pip install PyYAML

    SHELL

    test.vm.provision "file", source: "./cassandra_backup_service.py", destination: "/tmp/cassandra_backup_service.py"
    test.vm.provision "file", source: "./dry_run_full_test.py", destination: "/tmp/dry_run_full_test.py"

    test.vm.provision "shell", inline: <<-SHELL
      python /tmp/cassandra_backup_service.py full aws --aws-s3-bucket s3://test-bucket --dry-run
      python /tmp/dry_run_full_test.py
    SHELL

  end

end
