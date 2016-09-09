class baseconfig {
    exec { 'apt-get update':
        command => '/usr/bin/apt-get update';
    }
}
