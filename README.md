# New Relic Cassandra Plugin - Java
[![Build Status](https://travis-ci.org/thoersch/new-relic-cassandra.svg?branch=master)](https://travis-ci.org/thoersch/new-relic-cassandra)

Plugin is currently work in progress, with the focus of supporting Cassandra 2.x/3.x and JMX Authentication.

More metrics coming!

----

## Requirements

- A New Relic account. Sign up for a free account [here](http://newrelic.com)
- Java Runtime (JRE) environment Version 1.8+
- Network access to New Relic
- Cassandra 2.x/3.x

----

## Installation

You can either clone and build locally from source or,
download the [tar file](https://github.com/thoersch/new-relic-cassandra/tree/master/dist)

----

## Configuration Information

### Configuration Files

You will need to modify two configuration files in order to set this plugin up to run.  The first (`newrelic.json`) contains configurations used by all Platform plugins (e.g. license key, logging information, proxy settings) and can be shared across your plugins.  The second (`plugin.json`) contains data specific to each plugin such as a list of hosts and port combination for what you are monitoring.  Templates for both of these files should be located in the '`config`' directory in your extracted plugin folder. 

#### Configuring the `plugin.json` file: 

The `plugin.json` file has a provided template in the `config` directory named `plugin.template.json`. 
If you are installing manually, make a copy of this template file and rename it to `plugin.json` (the New Relic Platform Installer will automatically handle creation of configuration files for you).  

Below is an example of the `plugin.json` file's contents, you can add multiple objects to the "agents" array to monitor different instances:

```
{
  "agents": [
    {
      "name": "Cassandra",
      "host": "ip address of a cassandra node or comma delimited list",
      "port": "7199",
      "username": "optional jmx username",
      "password": "optional jmx password",
      "version": "2 or 3"
    }
  ]
}
```

**note** - The "name" attribute is used to identify specific instances in the New Relic UI.

**note** - The "host" attribute can be a single instance or list. 
Only one is required to get information on all hosts, but multiple are allow as a fall back if JMX fails to connect. 
Each of these specified will require connectivity from the plugin.

**note** - The "username" and "password" attributes can be left empty if not using JMX authentication

**note** - The "version" only supports 2 or 3, ignoring minor versions. If none is specified, it will default to 2.

#### Configuring the `newrelic.json` file: 

The `newrelic.json` file also has a provided template in the `config` directory named `newrelic.template.json`.  If you are installing manually, make a copy of this template file and rename it to `newrelic.json` (again, the New Relic Platform Installer will automatically handle this for you).  

The `newrelic.json` is a standardized file containing configuration information that applies to any plugin (e.g. license key, logging, proxy settings), so going forward you will be able to copy a single `newrelic.json` file from one plugin to another.  Below is a list of the configuration fields that can be managed through this file:

##### Configuring your New Relic License Key

Your New Relic license key is the only required field in the `newrelic.json` file as it is used to determine what account you are reporting to.  If you do not know what your license key is, you can learn about it [here](https://newrelic.com/docs/subscriptions/license-key).

Example: 

```
{
  "license_key": "YOUR_LICENSE_KEY_HERE"
}
```

----

## Support

Plugin support and troubleshooting assistance can be obtained by visiting [git issue tracking](https://github.com/thoersch/new-relic-cassandra/issues)

### Frequently Asked Questions

**Q: I've started this plugin, now what?**

**A:** Once you have a plugin reporting with the proper license key, log into New Relic [here](http://rpm.newrelic.com).  If everything was successful, you should see a new navigation item appear on the left navigation bar identifying your new plugin (This may take a few minutes).  Click on this item to see the metrics for what you were monitoring (bear in mind, some details -- such as summary metrics -- may take several minutes to show values).

----

## Contributing

Pull requests welcome!
