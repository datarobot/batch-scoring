# To install we basically just need to get the executable onto the system PATH.

# Linux / OSX users

# 1. Unzip or untar the executables 

  unzip datarobot_batch_scoring_*_executables.Linux.x86_64.zip    # for zip
  tar -xf datarobot_batch_scoring_*_executables.Linux.x86_64.tar  # for tar 


# 2. Install 
  # 2.A   Install as normal user without sudo

    # if you don't have sudo or want to install as a normal user, follow these directions
    # Skip to 2.B if you want to install with sudo 
  
    mkdir -p ~/bin/                                        # make bin dir for user
    cp datarobot_batch_scoring_*/batch_scoring* ~/bin/     # copy to bin dir
    echo 'export PATH=$PATH:~/bin' >> ~/.bashrc            # add bin to path
    source ~/.bashrc                                       # source path
    batch_scoring --help                                   # test that it worked

  # 2.B   Install with sudo
    
    sudo cp datarobot_batch_scoring_*/batch_scoring* /bin/ # copy to bin directory
    batch_scoring --help                                   # test that it worked


