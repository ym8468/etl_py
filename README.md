# etl_py
A lightweight framework for ETL work written in Python2.7

Welcome to the etl_py wiki!

## Overview
The etl_py is a lightweight python framework that can be reused for ETL (extract/transform/load) work.

You can easily making use of the existed actions for your customized requirement by adding/updating configure files.

You can easily add new actions and modules to support new features based on this framework.

## Package Dependency
Need to install the python package [JPype](https://github.com/originell/jpype) for JDBC module.

## About the design
MVC model (but there is no view part in this framework yet).
* etl.py: the entrance of all the actions
* actions: actions related parts. Actions mean extract, transform, load, copyfile, removefile, etc.
* modules: module related parts. Modules mean mysql, jdbc, etc.
* cfg files: configure settings (cfg files are in conf/actions/action_name/)
* conf/globals.cfg: contains all PATH settings
* modules/base/constants.py: contains all CONTANTS like action/section/option names.

## How to use
### command design
`python etl.py --action=<action_name> --cfg=<cfg_file_name> --section=<section_name_pattern>  <additional_options>`

### examples
`python etl.py --action=sample --cfg=sample_config --section=section1`

### sample code
Please take a look at the following files:

`actions/sample.py`

`action/module/configure files`

`conf/modules/sample_folder/sample_module.cfg`

`conf/actions/sample/sample_config.cfg`

### available actions
There are already some actions and modules available for using:

`python etl.py --action=transform --cfg=sample1 --section=sample*`

`python etl.py --action=copyfile --cfg=sample1 --section=sample*`

`python etl.py --action=createLockfile --cfg=sample1 --section=sample*`

`python etl.py --action=deleteLockfile --cfg=sample1 --section=sample*`

`python etl.py --action=existsfile --cfg=sample1 --section=sample*`

`python etl.py --action=movefile --cfg=sample1 --section=sample*`

`python etl.py --action=removefile --cfg=sample1 --section=sample*`

`python etl.py --action=simpleLoad --cfg=sample1 --section=sample* --mode=full`

`python etl.py --action=simpleExtract --cfg=sample1 --section=sample* --mode=full`

To get the list of all actions, please type the following command:

`python etl.py`

## Create a new action & module
### Descripton
The code in actions/action_group.py do the following 3 things:

1. reading parameters

2. calling functions in modules

3. processing the return code/value

### Note
* The action/sample.py is a sample code which can tell you how to create a new action.
* Implement the core function in modules, which can be re-used by other actions.
* Add a conf/actions/action_name/sample.cfg for a new action to describe the usage of all related parameters.
