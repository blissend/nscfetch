from __future__ import absolute_import, unicode_literals
import sys
import json
import datetime
import traceback

# Nitro SDK doesn't support getting bulkbindings on gslbvserver/lbvserver
import requests

# Redis is the storage backend
import redis

# HashiCorp Vault
import hvac 

# Official Netscaler Nitro SDK
#print sys.path
#sys.path.insert(0, '/usr/local/python/lib/python2.7/site-packages/nitro_python-1.0-py2.7.egg')
#print sys.path
from nssrc.com.citrix.netscaler.nitro.exception.nitro_exception import nitro_exception
from nssrc.com.citrix.netscaler.nitro.resource.stat.gslb.gslbvserver_stats import gslbvserver_stats
from nssrc.com.citrix.netscaler.nitro.resource.config.gslb.gslbvserver_gslbservice_binding import gslbvserver_gslbservice_binding
from nssrc.com.citrix.netscaler.nitro.resource.stat.lb.lbvserver_stats import lbvserver_stats
from nssrc.com.citrix.netscaler.nitro.resource.config.lb.lbvserver_binding import lbvserver_binding
from nssrc.com.citrix.netscaler.nitro.service.nitro_service import nitro_service

class NSCFetch():
    """
    Fetches data from netscaler to populate into redis

    Only collects names of vservers, services and their states for both GSLB and LB
    """

    def __init__(self):
        self.redis_host = ""
        self.redis_port = 6379
        self.redis_db = 1
        self.vault_url = ""
        self.vault_role_id = ""
        self.vault_secret_id = ""
        self.vault_secret_path = ""
        self.domain = "" # used incase we need to provide .domain.com to hostnames
        self.protocol = "HTTPS" # sometimes netscaler needs HTTP instead

    def connect(self, nsc):
        try:
            # Get Netscaler User/Pass
            client = hvac.Client(url=self.vault_url) # supports verify=false
            data = client.write(
                'auth/approle/login', 
                role_id=self.vault_role_id, 
                secret_id=self.vault_secret_id)
            client.token = data['auth']['client_token']
            secret = client.read(self.vault_secret_path)
            self.nsc_user = str(secret['data']['username'])
            self.nsc_passwd = str(secret['data']['password'])

            # Create connection to redis
            self.redis = redis.StrictRedis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db)

            # Create instance of nitro_service for netscaler connection
            self.session = nitro_service(nsc+self.domain, self.protocol)
            user = self.nsc_user
            #if 'dmz' in nsc.lower():
                #user += '1'
            self.session.set_credential(user, self.nsc_passwd)
            self.session.timeout = 300

            # Log into netscaler using credentials
            self.session.login()

            return True

        except nitro_exception as e:
            traceback.print_stack()
            print(sys.exc_info()[0])
            print("Exception::errorcode="+str(e.errorcode)+",message="+ e.message)
            return False
        except Exception as e:
            traceback.print_stack()
            print(sys.exc_info()[0])
            print("Exception::message="+str(e.args))
            return False

    def disconnect(self):
        # Logout from the NetScaler appliance
        self.session.logout()

    def vserver(self, nsc, nsc_data="lb"):
        # Initialize (only two netscaler data types we deal with)
        data = []
        if nsc_data != "lb":
            redis_key = "gslb"
        else:
            redis_key = "lbvserver"

        # Make sure we can connect
        if self.connect(nsc) == False:
            return False

        # Use Nitro SDK to retreive all vservers
        try:
            if nsc_data != "lb":
                results = gslbvserver_stats.get(self.session)
            else:
                results = lbvserver_stats.get(self.session)
        except nitro_exception as  e:
            traceback.print_stack()
            print("Issue with {!s}".format(nsc))
            print(sys.exc_info()[0])
            print("Exception::errorcode="+str(e.errorcode)+",message="+ e.message)
            self.disconnect()
            return False

        # Build vserver list and store name and state into redis
        for r in results:
            if len(r.name) > 0:
                vserver = r.name
                state = r.state
                data.append(vserver)
                self.redis.set("nsc>"+nsc+">{!s}>".format(redis_key)+vserver, state)

        # Delete nonexisting vservers in redis
        for key in self.redis.keys("nsc>"+nsc+">{!s}>*".format(redis_key)):
            vserver = key.split('>')[3]
            if vserver not in data:
                self.redis.delete(key)

        # Dump GSLB vserver list into redis
        if len(data) > 0:
            self.redis.set("nsc>"+nsc+">{!s}".format(redis_key), json.dumps(data))

        self.disconnect()
        return True

    def service(self, nsc, nsc_data="lb"):
        # Initialize (only two netscaler types we use gslb or lb)
        if nsc_data != "lb":
            redis_key = "gslb"
            nsc_data = "gslb" # whatever accidently entered is corrected
            api_path = "gslbvserver_gslbservice_binding"
        else:
            redis_key = "lbvserver"
            api_path = "lbvserver_service_binding"

       # Always try to get vserver into redis before getting bindings
        if self.vserver(nsc, nsc_data) == False:
            return

        # Log into netscaler via Nitro API
        payload = json.dumps({
            'login': {
                'username': self.nsc_user, 
                'password': self.nsc_passwd,
                'timeout': '60'
            }
        })
        headers = {'Content-Type': 'application/json'}
        login_post = requests.post(
            "http://{!s}{!s}/nitro/v1/config/login".format(nsc, self.domain), 
            data=payload, 
            headers=headers
        )
        cookie = ""

        # Confirm login (201 status means session token created)
        if login_post.status_code == 201:

            # Get the bindings in bulk
            # https://www.citrix.com/blogs/2014/02/04/using-curl-with-the-netscaler-nitro-rest-api/
            cookie = 'NITRO_AUTH_TOKEN=%23%23' + login_post.json()['sessionid'][2:] + '; path=/nitro/v1'
            headers['Content-Type'] = 'application/vnd.com.citrix.netscaler.login+json'
            headers['Cookie'] = cookie
            data_get = requests.get(
                "http://{!s}{!s}/nitro/v1/config/{!s}?bulkbindings=yes".format(nsc, self.domain, api_path),
                headers=headers
            )

            # Confirm request success (200 means request was good)
            if data_get.status_code == 200:
                # Get old bindings first
                old_bindings = self.redis.keys("nsc>"+nsc+">{!s}>*>service>*".format(redis_key))

                # Finally loop through bulk bindings and store into redis
                list_binding = {}
                for binding in data_get.json()[api_path]:
                    if binding['name'] not in list_binding:
                        list_binding[binding['name']] = []
                    else:
                        list_binding[binding['name']].append(binding['servicename'])
                    self.redis.set(
                        "nsc>"+nsc+">{!s}>".format(redis_key)+binding['name']+">service>"+binding['servicename'], 
                        binding['curstate'])

                # Delete nonexisting services in redis
                new_bindings = self.redis.keys("nsc>"+nsc+">{!s}>*>service>*".format(redis_key))
                for key in old_bindings:
                    if key not in new_bindings:
                        self.redis.delete(key)

                # Dump GSLB vserver service list into redis
                if len(list_binding) > 0:
                    for key, value in list_binding.items():
                        self.redis.set(
                            "nsc>"+nsc+">{!s}>".format(redis_key)+key+">service",
                            json.dumps(value))

                # Update the last update time into redis
                self.update_time()

            else:
                print(
                    "Issue with API get request for " +
                    "{!s}/nitro/v1/config/{!s}?bulkbindings=yes".format(nsc, api_path)
                )
                print(headers)
                print(data_get.text)

            # Logout out of session
            headers['Content-Type'] = 'application/json'
            logout_post = requests.post(
                "http://{!s}{!s}/nitro/v1/config/logout".format(nsc, self.domain), 
                headers=headers,
                data=json.dumps({'logout': {}})
            )
            if logout_post.status_code != 201:
                print(
                    "Issue with API post request for {!s}/nitro/v1/config/logout".format(nsc)
                )
                print(headers)
                print(logout_post.text)
                 
        else:
            print(
                "Issue with API post request for " +
                "{!s}/nitro/v1/config/login".format(nsc)
            )
            print(headers)
            print(login_post.text)

    def update_time(self):
        self.redis.set('last', str(datetime.datetime.now()))

