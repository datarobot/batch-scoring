# Instructions - Install batch_scoring using pip for an unprivilaged user

### Section 1 - unzip or untar the files

# you need a zip file that has a name like 
#    datarobot_batch_scoring_1.10.0_offlinebundle.zip
# you must have python 2.7 or python 3 installed, but pip is NOT required

# unzip the batch_scoring_offlinebundle zip file and change directory into batch_scoring_offlinebundle
# E.g. 

unzip datarobot_batch_scoring_1.10.0_offlinebundle.zip
cd batch_scoring_offlinebundle

### Section 2 - bootstrap pip 

# If you have pip, you can skip this section and go to Section 3
# you might also need to go throught these steps if pip is outdated

# run the following command to install pip in --user mode
# note you can use "python3" instead of "python" if that is an option

python get-pip.py --user --no-index --find-links=helper_packages/

# then install all of the helper packages which you may want to use
python -m pip install --user --no-index --find-links=helper_packages/ helper_packages/*

# This will install pip and a few other commands in ~/.local/bin 
# but that directory must be added to your path before you can use them
# On Linux this can be done by appending a line to your user's ~/.bashrc
# 

echo 'export PATH=~/.local/bin:$PATH' >> ~/.bashrc 

# then source the file if on Linux
source ~/.bashrc

# Now the user will always be able to use the commands:
pip --version
> pip 9.0.1 from /home/user/.local/lib/python2.7/site-packages (python 2.7)

### Section 3 - install datarobot_batch_scoring

# at this point we could create and use a virtualenv if we wanted to. If you 
# know what you are doing and want to use a virtualenv, go right ahead. 

# If using virtualenv:
    pip install --no-index --find-links=required_packages/ required_packages/* 
# Otherwise:
    pip install --user --no-index --find-links=required_packages/ required_packages/* 


### Section 4 - install datarobot_batch_scoring

batch_scoring --help

