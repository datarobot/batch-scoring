# To install we basically just need to get the executable onto the system PATH.

# Linux / OSX users

# 1. untar the executables 

  tar -xf datarobot_batch_scoring_*_executables.Linux.x86_64.tar  


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


# Windows users

# 1. unzip the executables 

  right-click > extract here
  This will unzip a directory. Inside the directory are these directions and two .exe files. 


# 2. move executables to user's path

  make a directory, for example:
    C:\Users\BILL\bin     # note that BILL would be replaced with your user name

  copy the two .exe files to the new directory

# 3. add the new directory to the user's path

  - Click the the start menu
  - In the search bar click the search bar and search for "path". 
  - You are looking for a menue item called "Edit environment variables for your account"
  - Open "Edit environment variables for your account"
  - Select "PATH" and click edit.
  - In the "Variable value" bar go to the end of the existing path and append a semicolon + your new dir location. 
  - For example, this would be added at the end of BILL's path:

      ;C:\Users\BILL\bin

    don't delete anything else in the path and make sure you have a semicolon.

  - Save and close
  - Close any existing cmd shell and open a new cmd shell. 
  - Try typing 'batch_scoring --help' in the command shell. The program should be found.
