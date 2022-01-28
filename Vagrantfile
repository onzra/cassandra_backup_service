Vagrant.configure("2") do |config|

  config.vm.define "test" do |test|
    test.vm.box = "centos/7"
    test.vm.provider "virtualbox" do |vb|
        vb.memory = 2048
    end

    # Install Cassandra.
    test.vm.provision "shell", inline: <<-SHELL
      sudo -s
      echo -e "[cassandra]\nname=Apache Cassandra\nbaseurl=https://www.apache.org/dist/cassandra/redhat/22x/\ngpgcheck=1\nrepo_gpgcheck=1\ngpgkey=https://www.apache.org/dist/cassandra/KEYS\n" > /etc/yum.repos.d/cassandra.repo
      yum -y install cassandra
      service cassandra start
      curl "https://bootstrap.pypa.io/pip/2.7/get-pip.py" -o "get-pip.py"
      python get-pip.py
      pip install PyYAML
    SHELL

    # Execute script with dry-run argument and then confirm creation of manifest in META_PATH using Python test script.
    test.vm.provision "shell", keep_color: true, inline: <<-SHELL
      export META_PATH="/tmp/onzra_cassandra_backup_service"
      python /vagrant/cassandra_backup_service.py full aws --aws-s3-bucket s3://test-bucket --aws-s3-metadata-bucket s3://test-bucket-for-metadata --meta-path $META_PATH --dry-run
      python /vagrant/dry_run_full_test.py
    SHELL

  end

end
