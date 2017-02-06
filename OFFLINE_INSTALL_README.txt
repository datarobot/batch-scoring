# These directions are for the offline install 

# There are two methods of offline install
  1. a zip file containing all the packages needed to install pip 
     and batch_scoring for an unprivalaged offline user
  2. a single-file executable made by PyInstaller that only depends on 
     libc.



1. Instructions - Install batch_scoring using pip for an unprivilaged user

# you need a zip file that has a name like 
#    datarobot_batch_scoring_1.10.0_offlinebundle.zip
# you must have python 2.7 or python 3 installed but pip is NOT required

# unzip the offlinebundle zip file and change directory into offlinebundle
# run the following command to install pip in --user mode

python get-pip.py --user --no-index --find-links=helper_packages/

# then install all of the helper packages which you may want to use
python -m pip install --user --no-index --find-links=helper_packages/ helper_packages/*

# This will install pip and a few other commands in ~/.local/bin 
# but that directory must be added to your path before you can use them
# On Linux this can be done by appending the following line to your user's ~/.bashrc
# On windows or OSX this will look different

export PATH=~/.local/bin/:$PATH

# then source the file if on Linux
source ~/.bashrc

# Now the user will always be able to use the commands:
pip --version
> pip 9.0.1 from /home/user/.local/lib/python2.7/site-packages (python 2.7)

# at this point we could create and use a virtualenv if we wanted to. If you 
# know what you are doing and want to use a virtualenv, go right ahead. 
# That being said, it might be easier for the user if they don't need to know 
# about virtualenvs so in the case we are installing it in --user mode. 

pip install --user --no-index --find-links=required_packages/ required_packages/* 

# test it out 

batch_scoring --version



2. Instructions - install single-file executable

# The single-file executable is easy to install, but it will fail if it isn't made correctly.
# Each executable must be built for the OS and OS version it will run on, so make 
# sure you communicate that information. 

# Once you have the executable, place it on the user's path. test them out by running
# them on the command line

batch_scoring --version
batch_scoring_sse --version
