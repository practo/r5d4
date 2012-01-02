# Read Me
## Prerequisites
### Core
* redis - `sudo apt-get install redis-server`
* flask - `sudo easy_install flask`
* redis-py - `sudo easy_install redis`

### Optional
* redis >=2.2 - For making UNIX socket connections; Better network latency
* hiredis - `sudo easy_install hiredis` - Faster Redis operations

### Deployment
* uwsgi - `sudo easy_install uwsgi`
* nginx >0.8 - `sudo apt-get install nginx` | Ubuntu 10.04 users use [nginx PPA](http://wiki.nginx.org/Install#Ubuntu_PPA)

## Installing (UWSGI + Nginx + Ubuntu)
* A sample upstart script for starting UWSGI running flask and analytics worker
  is inside `scripts/upstart/` folder.
* A sample nginx configuration file for reverse proxying to flask app is inside
  `scripts/nginx_conf/` folder
* Install the upstart and nginx config files in `/etc/init/` and
  `/etc/nginx/sites-enabled/` respectively.
* The server can be started using:
 * `sudo service uwsgi_r5d4 start`
 * `sudo service nginx start`

## Verifying Installation
The following are some trivial tests to check whether the installation succeeded

* `redis-cli publish AnalyticsWorkerCmd refresh` should return `(integer) 1`
